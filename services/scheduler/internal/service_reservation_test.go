package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// reservationNodes registers the given nodes and returns a service with the
// reservation ledger on or off. Every node has 100 GiB of allocatable memory,
// equal to its memory_total, so the capacity base is 100 GiB.
func newReservationService(t *testing.T, limit *config.NodeResourceLimit, ledger bool, store BindingStore, nodeIDs ...string) *Service {
	t.Helper()
	nodes := make([]Node, 0, len(nodeIDs))
	capacity := fixedCapacityProvider{}
	for _, id := range nodeIDs {
		nodes = append(nodes, Node{ID: id, Endpoint: "http://" + id})
		capacity[id] = NodeCapacity{AllocatableMemoryBytes: 100 * gib, Source: "kubernetes"}
	}
	if store == nil {
		store = NewInMemoryBindingStore(defaultObservedReportTTL)
	}
	opts := []ServiceOption{
		WithNodeResourceLimit(limit),
		WithNodeCapacityProvider(capacity),
	}
	if ledger {
		opts = append(opts, WithAdmissionReservation(config.DefaultAdmissionReservationTTL))
	}
	return NewService(
		zap.NewNop(),
		NewAtomicNodeRegistry(nodes, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		store,
		opts...,
	)
}

func heartbeatSnapshot(t *testing.T, service *Service, nodeID string, snapshot *schedulerv1.NodeSnapshot, sandboxIDs []string, paused []*schedulerv1.PausedSandboxResources) {
	t.Helper()
	snapshot.Status = schedulerv1.NodeStatus_NODE_STATUS_READY
	if _, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-" + nodeID,
		Snapshot:          snapshot,
		SandboxIds:        sandboxIDs,
		PausedSandboxes:   paused,
	}); err != nil {
		t.Fatalf("heartbeat %s failed: %v", nodeID, err)
	}
}

// emptyNode is the p-1 fixture: 100 GiB, 16 vCPU, nothing allocated.
func emptyNode() *schedulerv1.NodeSnapshot {
	return &schedulerv1.NodeSnapshot{MemoryTotalBytes: 100 * gib, CpuCount: 16}
}

func p1Limit() *config.NodeResourceLimit {
	return &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(80),
		MaxCPUAllocatedPercent:    uint32Ptr(200),
	}
}

func coldHint(memoryGiB uint64, cpuCount uint32) *schedulerv1.ScheduleRequestHint {
	return &schedulerv1.ScheduleRequestHint{Kind: &schedulerv1.ScheduleRequestHint_NewColdSandbox{
		NewColdSandbox: &schedulerv1.NewColdSandboxHint{MemoryMb: memoryGiB << 10, CpuCount: cpuCount},
	}}
}

type scheduleOutcomes struct {
	admitted     int
	unavailable  int
	other        []error
	reservations []string
}

// scheduleConcurrently fires n Schedule calls released together by one start
// channel, so every call races for the same snapshot without any sleep.
func scheduleConcurrently(service *Service, n int, hint *schedulerv1.ScheduleRequestHint) scheduleOutcomes {
	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		out   scheduleOutcomes
		start = make(chan struct{})
	)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			resp, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: hint})
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				out.admitted++
				out.reservations = append(out.reservations, resp.GetReservationId())
			case status.Code(err) == codes.Unavailable && status.Convert(err).Message() == "no nodes available":
				out.unavailable++
			default:
				out.other = append(out.other, err)
			}
		}()
	}
	close(start)
	wg.Wait()
	return out
}

// p-1 reproduction: without the ledger, three simultaneous 30 GiB creates on an
// empty 100 GiB node all read allocated=0 and all pass an 80% ceiling, the
// same mechanism that put 282 GiB on a 126 GiB node on 05/11.
func TestReservationDisabledReproducesConcurrentOvercommit(t *testing.T) {
	service := newReservationService(t, p1Limit(), false, nil, "node-a")
	heartbeatSnapshot(t, service, "node-a", emptyNode(), nil, nil)

	out := scheduleConcurrently(service, 3, coldHint(30, 4))
	if out.admitted != 3 || out.unavailable != 0 || len(out.other) != 0 {
		t.Fatalf("ledger off: admitted=%d unavailable=%d other=%v, want the historical 3/0", out.admitted, out.unavailable, out.other)
	}
	for _, id := range out.reservations {
		if id != "" {
			t.Fatalf("ledger off must not return a reservation id, got %q", id)
		}
	}
}

// Two simultaneous creates never pass the limit together: 30+30 fits in 80
// GiB, the third would reach 90 GiB and is refused.
func TestReservationStopsConcurrentCreatesAtMemoryLimit(t *testing.T) {
	service := newReservationService(t, p1Limit(), true, nil, "node-a")
	heartbeatSnapshot(t, service, "node-a", emptyNode(), nil, nil)

	out := scheduleConcurrently(service, 3, coldHint(30, 4))
	if out.admitted != 2 || out.unavailable != 1 || len(out.other) != 0 {
		t.Fatalf("admitted=%d unavailable=%d other=%v, want 2/1", out.admitted, out.unavailable, out.other)
	}
	seen := map[string]bool{}
	for _, id := range out.reservations {
		if id == "" || seen[id] {
			t.Fatalf("every admission needs a distinct reservation id, got %v", out.reservations)
		}
		seen[id] = true
	}
	memoryBytes, cpuCount, slots, count := service.reservations.Project("node-a")
	if memoryBytes != 60*gib || cpuCount != 8 || slots != 2 || count != 2 {
		t.Fatalf("reserved = %d/%d/%d/%d, want 60GiB/8/2/2", memoryBytes, cpuCount, slots, count)
	}
}

// The CPU ceiling (200% of 16 cores = 32 vCPU) also sees what is in flight:
// four 8 vCPU creates reach exactly 200%, the fifth would reach 250%.
func TestReservationStopsConcurrentCreatesAtCPULimit(t *testing.T) {
	service := newReservationService(t, p1Limit(), true, nil, "node-a")
	heartbeatSnapshot(t, service, "node-a", &schedulerv1.NodeSnapshot{MemoryTotalBytes: 100 * gib, CpuCount: 16}, nil, nil)

	out := scheduleConcurrently(service, 5, coldHint(4, 8))
	if out.admitted != 4 || out.unavailable != 1 || len(out.other) != 0 {
		t.Fatalf("admitted=%d unavailable=%d other=%v, want 4/1", out.admitted, out.unavailable, out.other)
	}
}

// The 05/11 vector on the CPU side: 60 vCPU already allocated on 16 cores
// with a 200% ceiling refuses a 4 vCPU create (64 is over 32).
func TestReservationRefusesCreateOverAllocatedCPU(t *testing.T) {
	service := newReservationService(t, p1Limit(), true, nil, "node-a")
	heartbeatSnapshot(t, service, "node-a", &schedulerv1.NodeSnapshot{
		MemoryTotalBytes: 100 * gib,
		CpuCount:         16,
		AllocatedCpu:     60,
	}, nil, nil)

	_, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(4, 4)})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("64 of 32 vCPU must be refused, got %v", err)
	}
	if _, _, _, count := service.reservations.Project("node-a"); count != 0 {
		t.Fatalf("a refused create must not reserve, got %d entries", count)
	}
}

// Negative control: an isolated create on an empty node is admitted on the
// same node with the same response as before the ledger existed.
func TestReservationIsolatedCreateAdmittedAsBefore(t *testing.T) {
	for _, ledger := range []bool{false, true} {
		service := newReservationService(t, p1Limit(), ledger, nil, "node-a")
		heartbeatSnapshot(t, service, "node-a", emptyNode(), nil, nil)

		resp, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)})
		if err != nil {
			t.Fatalf("ledger=%v: isolated create failed: %v", ledger, err)
		}
		if resp.GetNode().GetNodeId() != "node-a" || resp.GetNode().GetEndpoint() != "http://node-a" {
			t.Fatalf("ledger=%v: unexpected node %+v", ledger, resp.GetNode())
		}
		if (resp.GetReservationId() != "") != ledger {
			t.Fatalf("ledger=%v: reservation id %q", ledger, resp.GetReservationId())
		}
	}
}

// The projection works on a copy: the heartbeat snapshot in the registry is
// the same after a decision that reserved capacity.
func TestReservationDoesNotMutateObservedSnapshot(t *testing.T) {
	service := newReservationService(t, p1Limit(), true, nil, "node-a")
	heartbeatSnapshot(t, service, "node-a", emptyNode(), nil, nil)
	before := proto.Clone(service.nodes.PeekObserved("node-a"))

	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)}); err != nil {
		t.Fatalf("schedule failed: %v", err)
	}
	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)}); err != nil {
		t.Fatalf("second schedule failed: %v", err)
	}
	if after := service.nodes.PeekObserved("node-a"); !proto.Equal(before, after) {
		t.Fatalf("observed snapshot mutated:\nbefore=%v\nafter=%v", before, after)
	}
}

// Settling releases exactly one reservation: the next create fits again, and
// repeating the same settlement does not release a second one.
func TestReservationSettleReleasesOnlyItsOwnCapacity(t *testing.T) {
	service := newReservationService(t, p1Limit(), true, nil, "node-a")
	heartbeatSnapshot(t, service, "node-a", emptyNode(), nil, nil)

	first, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)})
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)}); err != nil {
		t.Fatalf("second create failed: %v", err)
	}
	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)}); status.Code(err) != codes.Unavailable {
		t.Fatalf("third create must be refused, got %v", err)
	}

	settle := &schedulerv1.SettleAdmissionRequest{ReservationId: first.GetReservationId(), Outcome: ReservationFailed}
	if _, err := service.SettleAdmission(context.Background(), settle); err != nil {
		t.Fatalf("settle failed: %v", err)
	}
	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)}); err != nil {
		t.Fatalf("a create must fit after one settlement: %v", err)
	}
	// A repeated settlement is a no-op, not a second release.
	if _, err := service.SettleAdmission(context.Background(), settle); err != nil {
		t.Fatalf("repeated settle must not fail: %v", err)
	}
	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)}); status.Code(err) != codes.Unavailable {
		t.Fatalf("a repeated settlement released another reservation: %v", err)
	}
}

func TestSettleAdmissionValidatesTheClosedOutcomeSet(t *testing.T) {
	service := newReservationService(t, p1Limit(), true, nil, "node-a")
	cases := []struct {
		name string
		req  *schedulerv1.SettleAdmissionRequest
		code codes.Code
	}{
		{"missing id", &schedulerv1.SettleAdmissionRequest{Outcome: ReservationConfirmed}, codes.InvalidArgument},
		{"unknown outcome", &schedulerv1.SettleAdmissionRequest{ReservationId: "reservation-1", Outcome: "done"}, codes.InvalidArgument},
		{"unknown id", &schedulerv1.SettleAdmissionRequest{ReservationId: "reservation-404", Outcome: ReservationConfirmed}, codes.OK},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := service.SettleAdmission(context.Background(), tc.req)
			if status.Code(err) != tc.code {
				t.Fatalf("code = %v, want %v (err=%v)", status.Code(err), tc.code, err)
			}
		})
	}
}

// A create whose template shape is unknown still reserves a slot, so
// max_sandbox_starting_count sees creates in flight.
func TestReservationShapelessCreateReservesAStartingSlot(t *testing.T) {
	limit := p1Limit()
	limit.MaxSandboxStartingCount = uint32Ptr(1)
	service := newReservationService(t, limit, true, nil, "node-a")
	heartbeatSnapshot(t, service, "node-a", emptyNode(), nil, nil)
	hint := &schedulerv1.ScheduleRequestHint{Kind: &schedulerv1.ScheduleRequestHint_NewSandbox{
		NewSandbox: &schedulerv1.NewSandboxHint{Metadata: map[string]string{"k": "v"}},
	}}

	out := scheduleConcurrently(service, 3, hint)
	if out.admitted != 2 || out.unavailable != 1 || len(out.other) != 0 {
		t.Fatalf("admitted=%d unavailable=%d other=%v, want 2/1", out.admitted, out.unavailable, out.other)
	}
	memoryBytes, cpuCount, slots, _ := service.reservations.Project("node-a")
	if memoryBytes != 0 || cpuCount != 0 || slots != 2 {
		t.Fatalf("shapeless reservation = %d/%d/%d, want 0/0/2", memoryBytes, cpuCount, slots)
	}
}

// A NewSandbox hint carrying memoryMB and cpuCount reserves that shape.
func TestReservationNewSandboxWithShapeReservesIt(t *testing.T) {
	service := newReservationService(t, p1Limit(), true, nil, "node-a")
	heartbeatSnapshot(t, service, "node-a", emptyNode(), nil, nil)
	hint := &schedulerv1.ScheduleRequestHint{Kind: &schedulerv1.ScheduleRequestHint_NewSandbox{
		NewSandbox: &schedulerv1.NewSandboxHint{MemoryMb: 30 << 10, CpuCount: 4},
	}}

	out := scheduleConcurrently(service, 3, hint)
	if out.admitted != 2 || out.unavailable != 1 {
		t.Fatalf("admitted=%d unavailable=%d other=%v, want 2/1", out.admitted, out.unavailable, out.other)
	}
}

// A request that does not allocate (no hint) reserves nothing.
func TestReservationNilHintReservesNothing(t *testing.T) {
	service := newReservationService(t, p1Limit(), true, nil, "node-a")
	heartbeatSnapshot(t, service, "node-a", emptyNode(), nil, nil)

	resp, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
	if err != nil {
		t.Fatalf("schedule failed: %v", err)
	}
	if resp.GetReservationId() != "" {
		t.Fatalf("a non-allocating request reserved %q", resp.GetReservationId())
	}
	if _, _, _, count := service.reservations.Project("node-a"); count != 0 {
		t.Fatalf("reservations = %d, want 0", count)
	}
}

// pausedOwner heartbeats the owner with 50 GiB allocated and a 16 GiB / 4 vCPU
// paused sandbox, and fills alt so creates can only land on the owner.
func pausedOwner(t *testing.T, service *Service, ownerAllocatedGiB uint64, altAllocatedGiB uint64) {
	t.Helper()
	heartbeatSnapshot(t, service, "owner", &schedulerv1.NodeSnapshot{
		MemoryTotalBytes:     100 * gib,
		AllocatedMemoryBytes: ownerAllocatedGiB * gib,
		MemoryUsedBytes:      ownerAllocatedGiB * gib,
		CpuCount:             16,
	}, []string{"sbx-1"}, []*schedulerv1.PausedSandboxResources{
		{SandboxId: "sbx-1", CpuCount: 4, MemoryBytes: 16 * gib},
	})
	heartbeatSnapshot(t, service, "alt", &schedulerv1.NodeSnapshot{
		MemoryTotalBytes:     100 * gib,
		AllocatedMemoryBytes: altAllocatedGiB * gib,
		MemoryUsedBytes:      altAllocatedGiB * gib,
		CpuCount:             16,
	}, nil, nil)
}

// A resume counts in the same reservation: once admitted, a concurrent 16 GiB
// create on the owner (50+16+16 > 80) is refused, and repeating the resume of
// the same sandbox replaces its reservation instead of doubling it.
func TestReservationResumeCountsAndDoesNotDouble(t *testing.T) {
	service := newReservationService(t, admissionLimit(false), true, nil, "owner", "alt")
	pausedOwner(t, service, 50, 90)

	for i := 0; i < 2; i++ {
		resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
		if err != nil {
			t.Fatalf("resume %d failed: %v", i, err)
		}
		if !resp.GetAdmitted() || resp.GetNode().GetNodeId() != "owner" || resp.GetReservationId() == "" {
			t.Fatalf("resume %d: unexpected response %+v", i, resp)
		}
	}
	memoryBytes, cpuCount, _, count := service.reservations.Project("owner")
	if memoryBytes != 16*gib || cpuCount != 4 || count != 1 {
		t.Fatalf("repeated resume reserved %d/%d/%d, want 16GiB/4/1", memoryBytes, cpuCount, count)
	}

	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(16, 4)}); status.Code(err) != codes.Unavailable {
		t.Fatalf("a create over the resumed reservation must be refused, got %v", err)
	}
	// Negative control: the same create fits on the owner without the resume.
	control := newReservationService(t, admissionLimit(false), true, nil, "owner", "alt")
	pausedOwner(t, control, 50, 90)
	if _, err := control.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(16, 4)}); err != nil {
		t.Fatalf("control create must be admitted: %v", err)
	}
}

// A deferred resume leaves no reservation behind.
func TestReservationDeferredResumeReservesNothing(t *testing.T) {
	service := newReservationService(t, admissionLimit(false), true, nil, "owner", "alt")
	pausedOwner(t, service, 70, 90)

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if resp.GetAdmitted() || resp.GetReservationId() != "" {
		t.Fatalf("expected a deferred resume without reservation, got %+v", resp)
	}
	for _, node := range []string{"owner", "alt"} {
		if _, _, _, count := service.reservations.Project(node); count != 0 {
			t.Fatalf("%s holds %d reservations after a deferral", node, count)
		}
	}
}

// With cross_node_resume on, the reservation lands on the destination: alt at
// 50 GiB plus the 16 GiB resume plus a 30 GiB create is over 80.
func TestReservationCrossNodeResumeReservesOnDestination(t *testing.T) {
	service := newReservationService(t, admissionLimit(true), true, nil, "owner", "alt")
	pausedOwner(t, service, 70, 50)

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if !resp.GetAdmitted() || resp.GetNode().GetNodeId() != "alt" || resp.GetReservationId() == "" {
		t.Fatalf("expected a move to alt with a reservation, got %+v", resp)
	}
	if memoryBytes, _, _, _ := service.reservations.Project("alt"); memoryBytes != 16*gib {
		t.Fatalf("alt reserved %d bytes, want 16 GiB", memoryBytes)
	}
	if _, _, _, count := service.reservations.Project("owner"); count != 0 {
		t.Fatalf("owner holds %d reservations, want 0", count)
	}
	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)}); status.Code(err) != codes.Unavailable {
		t.Fatalf("a create over the moved resume must be refused, got %v", err)
	}
}

// Alternative candidates are filtered with their reservations projected: an
// alt node whose free room is already reserved is not a resume destination.
func TestReservationCrossNodeResumeSeesDestinationReservations(t *testing.T) {
	service := newReservationService(t, admissionLimit(true), true, nil, "owner", "alt")
	pausedOwner(t, service, 70, 50)
	service.reservations.Reserve("alt", 20*gib, 4, 1, "")

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	if resp.GetAdmitted() {
		t.Fatalf("alt at 50+20+16 GiB must not admit the resume, got %+v", resp)
	}
}

type failingRecordStore struct {
	BindingStore
}

func (failingRecordStore) Record(string, Node, time.Time) error {
	return errors.New("redis down")
}

// A moved resume whose binding write fails releases its reservation.
func TestReservationMovedResumeReleasedWhenBindingWriteFails(t *testing.T) {
	store := failingRecordStore{BindingStore: NewInMemoryBindingStore(defaultObservedReportTTL)}
	service := newReservationService(t, admissionLimit(true), true, store, "owner", "alt")
	pausedOwner(t, service, 70, 50)

	_, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected Unavailable on a failed binding write, got %v", err)
	}
	if _, _, _, count := service.reservations.Project("alt"); count != 0 {
		t.Fatalf("a failed move left %d reservations on alt", count)
	}
}

// The published verdict reflects the reservation: with 50 GiB allocated and a
// 16 GiB resume in flight, the 16 GiB layer (82 of 80) is no longer admitted.
func TestReservationPublishedVerdictIncludesReservation(t *testing.T) {
	service := newReservationService(t, admissionLimit(false), true, nil, "owner", "alt")
	pausedOwner(t, service, 50, 90)

	layer16 := func() (bool, *schedulerv1.NodeAdmission) {
		resp, err := service.ListObservedNodes(context.Background(), &schedulerv1.ListObservedNodesRequest{ClusterId: "cluster-1"})
		if err != nil {
			t.Fatalf("list observed nodes failed: %v", err)
		}
		for _, node := range resp.GetNodes() {
			if node.GetNodeId() != "owner" {
				continue
			}
			for _, layer := range node.GetAdmission().GetLayers() {
				if layer.GetMemoryGb() == 16 {
					return layer.GetAdmitted(), node.GetAdmission()
				}
			}
		}
		t.Fatal("owner 16 GiB layer not published")
		return false, nil
	}

	if admitted, admission := layer16(); !admitted || admission.GetReservedMemoryBytes() != 0 {
		t.Fatalf("before the resume: admitted=%v reserved=%d", admitted, admission.GetReservedMemoryBytes())
	}
	if _, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"}); err != nil {
		t.Fatalf("resume failed: %v", err)
	}
	admitted, admission := layer16()
	if admitted {
		t.Fatal("the 16 GiB layer must not be admitted while the resume reservation exists")
	}
	if admission.GetReservedMemoryBytes() != 16*gib || admission.GetReservedCpu() != 4 {
		t.Fatalf("published reserved = %d/%d, want 16GiB/4", admission.GetReservedMemoryBytes(), admission.GetReservedCpu())
	}
	if admission.GetRuntimeOverheadBytes() != 0 {
		t.Fatalf("the projection must not change the runtime overhead, got %d", admission.GetRuntimeOverheadBytes())
	}
}

// A confirmed reservation keeps occupying the node until the first snapshot
// the node produces after the confirmation. Without it, a create arriving in
// the gap between the upstream 2xx and the next heartbeat finds the node
// looking free and reproduces the overflow the ledger exists to stop.
func TestReservationConfirmedHeldUntilNextHeartbeat(t *testing.T) {
	service := newReservationService(t, p1Limit(), true, nil, "node-a")
	initial := emptyNode()
	initial.ReportedAtUnixMs = time.Now().Add(-time.Minute).UnixMilli()
	heartbeatSnapshot(t, service, "node-a", initial, nil, nil)

	first, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)})
	if err != nil {
		t.Fatalf("first create failed: %v", err)
	}
	if _, err := service.SettleAdmission(context.Background(), &schedulerv1.SettleAdmissionRequest{
		ReservationId: first.GetReservationId(),
		Outcome:       ReservationConfirmed,
	}); err != nil {
		t.Fatalf("confirming the create failed: %v", err)
	}

	// The heartbeat still reports the pre-confirmation snapshot, so 30 GiB
	// reserved plus 60 GiB incoming is over the 80 GiB ceiling.
	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(60, 8)}); status.Code(err) != codes.Unavailable {
		t.Fatalf("a create in the confirm-to-heartbeat window must be refused, got %v", err)
	}

	// The next heartbeat counts the confirmed sandbox. The reservation is
	// released and only the observed 30 GiB remain, so another 30 GiB fits.
	observed := &schedulerv1.NodeSnapshot{
		MemoryTotalBytes:     100 * gib,
		AllocatedMemoryBytes: 30 * gib,
		MemoryUsedBytes:      30 * gib,
		CpuCount:             16,
		SandboxCount:         1,
		SandboxStartingCount: 1,
		ReportedAtUnixMs:     time.Now().Add(time.Second).UnixMilli(),
	}
	heartbeatSnapshot(t, service, "node-a", observed, []string{"sbx-1"}, nil)
	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(30, 4)}); err != nil {
		t.Fatalf("after the heartbeat the confirmed reservation must be released: %v", err)
	}
	memoryBytes, _, _, count := service.reservations.Project("node-a")
	if memoryBytes != 30*gib || count != 1 {
		t.Fatalf("after release only the new create must be reserved: %d bytes, %d entries", memoryBytes, count)
	}
}

// The live scheduler runs admission_mode shadow, whose effective decision is
// the nominal memory verdict. The reservation projection must feed that
// verdict, not only the "request" comparison the observer logs; otherwise the
// shadow deployment would still overcommit while claiming to measure.
func TestReservationProjectionFeedsShadowNominalDecision(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(65),
		AdmissionMode:             strPtr(config.AdmissionModeShadow),
	}
	observed := &schedulerv1.NodeSnapshot{
		MemoryTotalBytes:     100 * gib,
		AllocatedMemoryBytes: 60 * gib,
		MemoryUsedBytes:      60 * gib,
		// The request sum stays under the ceiling, so only the allocated
		// figure can reject the node and the two verdicts genuinely diverge.
		RequestedMemoryBytes: 10 * gib,
		CpuCount:             16,
	}
	// A shapeless create skips filterByShape, so only the nominal resource
	// limit can reject the node. The 16 GiB reservation takes allocated to 76%.
	shapeless := &schedulerv1.ScheduleRequestHint{Kind: &schedulerv1.ScheduleRequestHint_NewSandbox{
		NewSandbox: &schedulerv1.NewSandboxHint{Metadata: map[string]string{"k": "v"}},
	}}

	service := newReservationService(t, limit, true, nil, "node-a")
	heartbeatSnapshot(t, service, "node-a", observed, nil, nil)
	service.reservations.Reserve("node-a", 16*gib, 4, 1, "")

	// The projected node is rejected by the effective decision, and the shadow
	// comparison proves it was the nominal verdict (allocated 76%) doing the
	// rejecting while the logged request verdict (requested 26%) would admit.
	// A projection that reached only the observer would leave nominal at 60%.
	projected := service.projectReservations([]RichNode{service.richNode("node-a")})
	admitted, events := shadowRun(projected, limit)
	if len(admitted) != 0 {
		t.Fatalf("shadow mode must reject the node on the projected nominal verdict, got %d admitted nodes", len(admitted))
	}
	if len(events) != 1 || events[0] != (shadowEvent{"node-a", false, true}) {
		t.Fatalf("shadow divergence = %+v, want node-a nominal=false request=true", events)
	}
	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: shapeless}); status.Code(err) != codes.Unavailable {
		t.Fatalf("shadow mode must refuse the create on the projected nominal verdict, got %v", err)
	}

	// Negative control: without the reservation the same nominal verdict
	// admits the node (60% is under the 65% ceiling).
	control := newReservationService(t, limit, false, nil, "node-a")
	heartbeatSnapshot(t, control, "node-a", observed, nil, nil)
	if _, err := control.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: shapeless}); err != nil {
		t.Fatalf("control create must be admitted in shadow mode: %v", err)
	}
}
