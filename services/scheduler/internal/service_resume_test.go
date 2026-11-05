package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
)

type fixedCapacityProvider map[string]NodeCapacity

func (p fixedCapacityProvider) Capacity(nodeID string) (NodeCapacity, bool) {
	capacity, ok := p[nodeID]
	return capacity, ok
}

func heartbeatWithPaused(
	t *testing.T,
	service *Service,
	nodeID string,
	sandboxID string,
	allocatedGiB uint64,
	pausedMemoryGiB uint64,
) {
	t.Helper()
	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-" + nodeID,
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:               schedulerv1.NodeStatus_NODE_STATUS_READY,
			MemoryTotalBytes:     100 * gib,
			AllocatedMemoryBytes: allocatedGiB * gib,
			MemoryUsedBytes:      allocatedGiB * gib,
			CpuCount:             16,
		},
		SandboxIds: []string{sandboxID},
		PausedSandboxes: []*schedulerv1.PausedSandboxResources{
			{SandboxId: sandboxID, CpuCount: 4, MemoryBytes: pausedMemoryGiB * gib},
		},
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
}

func newResumeService(t *testing.T, provider NodeCapacityProvider, limit *config.NodeResourceLimit) (*Service, *AtomicNodeRegistry) {
	t.Helper()
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "owner", Endpoint: "http://owner"},
		{ID: "alt", Endpoint: "http://alt"},
	}, defaultObservedReportTTL)
	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
		WithNodeResourceLimit(limit),
		WithNodeCapacityProvider(provider),
	)
	return service, registry
}

func admissionLimit(crossNode bool) *config.NodeResourceLimit {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(80),
	}
	if crossNode {
		limit.CrossNodeResume = boolPtr(true)
	}
	return limit
}

func twoNodesCapacity() fixedCapacityProvider {
	return fixedCapacityProvider{
		"owner": {AllocatableMemoryBytes: 100 * gib, Source: "kubernetes"},
		"alt":   {AllocatableMemoryBytes: 100 * gib, Source: "kubernetes"},
	}
}

// The owner has room for the 16 GiB paused shape: the resume stays where the
// binding already points.
func TestScheduleResumeStaysOnAdmittedOwner(t *testing.T) {
	service, _ := newResumeService(t, twoNodesCapacity(), admissionLimit(false))
	heartbeatWithPaused(t, service, "owner", "sbx-1", 50, 16)

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("schedule resume failed: %v", err)
	}
	if !resp.GetAdmitted() || resp.GetNode().GetNodeId() != "owner" || resp.GetReason() != "" {
		t.Fatalf("unexpected resume response: %+v", resp)
	}
}

// Owner at 70 of an 80 GiB ceiling plus a 16 GiB shape is rejected; with
// cross-node resume on, the alternative at 50 GiB is admitted and the binding
// moves only after selection.
func TestScheduleResumeMovesWhenOwnerIsFull(t *testing.T) {
	service, _ := newResumeService(t, twoNodesCapacity(), admissionLimit(true))
	heartbeatWithPaused(t, service, "owner", "sbx-1", 70, 16)
	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "alt",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-alt",
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:               schedulerv1.NodeStatus_NODE_STATUS_READY,
			MemoryTotalBytes:     100 * gib,
			AllocatedMemoryBytes: 50 * gib,
			MemoryUsedBytes:      50 * gib,
		},
	})
	if err != nil {
		t.Fatalf("alt heartbeat failed: %v", err)
	}

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("schedule resume failed: %v", err)
	}
	if !resp.GetAdmitted() || resp.GetNode().GetNodeId() != "alt" {
		t.Fatalf("expected the alt node, got %+v", resp)
	}
	node, ok, _ := service.store.Get("sbx-1", time.Now())
	if !ok || node.ID != "alt" {
		t.Fatalf("binding must move to alt, got %+v ok=%v", node, ok)
	}
}

// With cross-node resume off the resume is deferred with the owner's closed
// reason and the binding is untouched.
func TestScheduleResumeDefersWhenOwnerIsFullAndCrossNodeOff(t *testing.T) {
	service, _ := newResumeService(t, twoNodesCapacity(), admissionLimit(false))
	heartbeatWithPaused(t, service, "owner", "sbx-1", 70, 16)

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("schedule resume failed: %v", err)
	}
	if resp.GetAdmitted() || resp.GetNode().GetNodeId() != "owner" || resp.GetReason() != ReasonMemoryAllocated {
		t.Fatalf("expected a deferred resume with memory_allocated, got %+v", resp)
	}
	node, ok, _ := service.store.Get("sbx-1", time.Now())
	if !ok || node.ID != "owner" {
		t.Fatalf("binding must stay on the owner, got %+v ok=%v", node, ok)
	}
}

// An unknown shape fails closed: the resume is deferred instead of
// over-admitting the node.
func TestScheduleResumeDefersOnUnknownShape(t *testing.T) {
	service, _ := newResumeService(t, twoNodesCapacity(), admissionLimit(true))
	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "owner",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-owner",
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:               schedulerv1.NodeStatus_NODE_STATUS_READY,
			MemoryTotalBytes:     100 * gib,
			AllocatedMemoryBytes: 50 * gib,
		},
		SandboxIds: []string{"sbx-1"},
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("schedule resume failed: %v", err)
	}
	if resp.GetAdmitted() || resp.GetReason() != "" {
		t.Fatalf("unknown shape must defer with no reason, got %+v", resp)
	}
}

// The gateway-supplied template shape is used when the heartbeat does not
// report per-sandbox paused resources yet.
func TestScheduleResumeUsesGatewayShapeWhenHeartbeatIsSilent(t *testing.T) {
	service, _ := newResumeService(t, twoNodesCapacity(), admissionLimit(false))
	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "owner",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-owner",
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:               schedulerv1.NodeStatus_NODE_STATUS_READY,
			MemoryTotalBytes:     100 * gib,
			AllocatedMemoryBytes: 70 * gib,
			MemoryUsedBytes:      70 * gib,
		},
		SandboxIds: []string{"sbx-1"},
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{
		SandboxId:   "sbx-1",
		MemoryBytes: 16 * gib,
		CpuCount:    4,
	})
	if err != nil {
		t.Fatalf("schedule resume failed: %v", err)
	}
	if resp.GetAdmitted() || resp.GetReason() != ReasonMemoryAllocated {
		t.Fatalf("gateway shape must be projected, got %+v", resp)
	}
}

// The shape_admission off kill switch restores the unconditional owner route.
func TestScheduleResumeKillSwitchAdmitsOwner(t *testing.T) {
	limit := admissionLimit(false)
	limit.ShapeAdmission = strPtr(config.ShapeAdmissionOff)
	service, _ := newResumeService(t, twoNodesCapacity(), limit)
	heartbeatWithPaused(t, service, "owner", "sbx-1", 99, 16)

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
	if err != nil {
		t.Fatalf("schedule resume failed: %v", err)
	}
	if !resp.GetAdmitted() || resp.GetNode().GetNodeId() != "owner" {
		t.Fatalf("kill switch must admit the owner, got %+v", resp)
	}
}

func TestScheduleResumeRejectsUnknownSandbox(t *testing.T) {
	service, _ := newResumeService(t, twoNodesCapacity(), admissionLimit(false))
	if _, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "missing"}); err == nil {
		t.Fatal("unknown sandbox must be NotFound")
	}
}

func TestListObservedNodesCarriesAdmissionVerdicts(t *testing.T) {
	service, _ := newResumeService(t, fixedCapacityProvider{
		"owner": {AllocatableMemoryBytes: 100 * gib, Source: "kubernetes"},
	}, &config.NodeResourceLimit{MaxMemoryAllocatedPercent: uint32Ptr(80)})
	heartbeatWithPaused(t, service, "owner", "sbx-1", 70, 16)

	resp, err := service.ListObservedNodes(context.Background(), &schedulerv1.ListObservedNodesRequest{ClusterId: "cluster-1"})
	if err != nil {
		t.Fatalf("list observed nodes failed: %v", err)
	}
	if len(resp.GetNodes()) != 1 {
		t.Fatalf("got %d nodes, want 1", len(resp.GetNodes()))
	}
	admission := resp.GetNodes()[0].GetAdmission()
	if admission == nil {
		t.Fatal("expected an admission verdict")
	}
	if admission.GetCapacitySource() != config.MemoryCapacitySourceAllocatable {
		t.Fatalf("capacity source = %q", admission.GetCapacitySource())
	}
	if admission.GetCapacityMemoryBytes() != 100*gib {
		t.Fatalf("capacity = %d, want 100 GiB", admission.GetCapacityMemoryBytes())
	}
	if len(admission.GetLayers()) != 4 {
		t.Fatalf("got %d layers, want 4", len(admission.GetLayers()))
	}
	// 70 GiB allocated plus 16 GiB = 86% > 80%.
	byLayer := map[uint64]*schedulerv1.AdmissionVerdict{}
	for _, layer := range admission.GetLayers() {
		byLayer[layer.GetMemoryGb()] = layer
	}
	if got := byLayer[16]; got == nil || got.GetAdmitted() || got.GetReason() != ReasonMemoryAllocated {
		t.Fatalf("16 GiB verdict = %+v, want rejected memory_allocated", got)
	}
	if got := byLayer[8]; got == nil || !got.GetAdmitted() || got.GetReason() != "" {
		t.Fatalf("8 GiB verdict = %+v, want admitted with no reason", got)
	}
}

func TestListObservedNodesOmitsAdmissionWhenShapeAdmissionOff(t *testing.T) {
	service, _ := newResumeService(t, twoNodesCapacity(), &config.NodeResourceLimit{
		ShapeAdmission: strPtr(config.ShapeAdmissionOff),
	})
	heartbeatWithPaused(t, service, "owner", "sbx-1", 10, 8)

	resp, err := service.ListObservedNodes(context.Background(), &schedulerv1.ListObservedNodesRequest{ClusterId: "cluster-1"})
	if err != nil {
		t.Fatalf("list observed nodes failed: %v", err)
	}
	if got := resp.GetNodes()[0].GetAdmission(); got != nil {
		t.Fatalf("shape_admission off must omit the verdict, got %+v", got)
	}
}
