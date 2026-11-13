package scheduler

import (
	"context"
	"strings"
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// heartbeatWithPool registers one node and reports the pool it belongs to.
func heartbeatWithPool(t *testing.T, service *Service, nodeID, pool string, sandboxCount uint32) {
	t.Helper()
	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-" + nodeID,
		Pool:              pool,
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:               schedulerv1.NodeStatus_NODE_STATUS_READY,
			SandboxCount:         sandboxCount,
			MemoryTotalBytes:     100 * gib,
			MemoryUsedBytes:      10 * gib,
			AllocatedMemoryBytes: 10 * gib,
			CpuCount:             16,
		},
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
}

func newPoolService(t *testing.T, limit *config.NodeResourceLimit) (*Service, *AtomicNodeRegistry) {
	t.Helper()
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "shared", Endpoint: "http://shared"},
		{ID: "spot-a", Endpoint: "http://spot-a"},
		{ID: "spot-b", Endpoint: "http://spot-b"},
	}, defaultObservedReportTTL)
	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
		WithNodeResourceLimit(limit),
	)
	return service, registry
}

func newSandboxRequest(pool string) *schedulerv1.ScheduleRequest {
	return &schedulerv1.ScheduleRequest{
		Hint: &schedulerv1.ScheduleRequestHint{
			Kind: &schedulerv1.ScheduleRequestHint_NewSandbox{
				NewSandbox: &schedulerv1.NewSandboxHint{},
			},
			Pool: pool,
		},
	}
}

func TestFilterByPool(t *testing.T) {
	nodes := []Node{
		{ID: "shared", Endpoint: "http://shared"},
		{ID: "spot-a", Endpoint: "http://spot-a"},
		{ID: "spot-b", Endpoint: "http://spot-b"},
	}
	pools := map[string]string{"shared": "", "spot-a": "spot", "spot-b": "other"}
	nodePool := func(nodeID string) string { return pools[nodeID] }

	t.Run("empty pool matches only nodes without pool", func(t *testing.T) {
		got := FilterByPool(nodes, "", nodePool)
		if len(got) != 1 || got[0].ID != "shared" {
			t.Fatalf("expected only the shared node, got %v", got)
		}
	})

	t.Run("named pool matches only its nodes", func(t *testing.T) {
		got := FilterByPool(nodes, "spot", nodePool)
		if len(got) != 1 || got[0].ID != "spot-a" {
			t.Fatalf("expected only spot-a, got %v", got)
		}
	})

	t.Run("unknown pool matches nothing", func(t *testing.T) {
		if got := FilterByPool(nodes, "missing", nodePool); len(got) != 0 {
			t.Fatalf("expected no node, got %v", got)
		}
	})
}

// A sandbox that did not ask for a pool never lands on a node that belongs to
// one: the shared node is the only candidate.
func TestScheduleSkipsPoolNodesForSharedRequest(t *testing.T) {
	service, _ := newPoolService(t, nil)
	heartbeatWithPool(t, service, "shared", "", 0)
	heartbeatWithPool(t, service, "spot-a", "spot", 0)
	heartbeatWithPool(t, service, "spot-b", "other", 0)

	for i := 0; i < 6; i++ {
		resp, err := service.Schedule(context.Background(), newSandboxRequest(""))
		if err != nil {
			t.Fatalf("schedule failed: %v", err)
		}
		if got := resp.GetNode().GetNodeId(); got != "shared" {
			t.Fatalf("iteration %d: expected shared, got %q", i, got)
		}
	}
}

// A sandbox that asked for a pool only lands on nodes of that pool, never on
// the shared nodes and never on another pool.
func TestScheduleSelectsOnlyRequestedPool(t *testing.T) {
	service, _ := newPoolService(t, nil)
	heartbeatWithPool(t, service, "shared", "", 0)
	heartbeatWithPool(t, service, "spot-a", "spot", 0)
	heartbeatWithPool(t, service, "spot-b", "spot", 0)

	seen := map[string]bool{}
	for i := 0; i < 6; i++ {
		resp, err := service.Schedule(context.Background(), newSandboxRequest("spot"))
		if err != nil {
			t.Fatalf("schedule failed: %v", err)
		}
		id := resp.GetNode().GetNodeId()
		if id != "spot-a" && id != "spot-b" {
			t.Fatalf("iteration %d: expected a spot node, got %q", i, id)
		}
		seen[id] = true
	}
	if !seen["spot-a"] || !seen["spot-b"] {
		t.Fatalf("expected both spot nodes to be candidates, saw %v", seen)
	}
}

// When no node of the requested pool exists, the capacity refusal names the
// pool instead of the generic "no nodes available".
func TestScheduleRefusesWhenPoolHasNoNode(t *testing.T) {
	service, _ := newPoolService(t, nil)
	heartbeatWithPool(t, service, "shared", "", 0)

	_, err := service.Schedule(context.Background(), newSandboxRequest("spot"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable, got %v", err)
	}
	if !strings.Contains(status.Convert(err).Message(), "spot") {
		t.Fatalf("expected the refusal to name the pool, got %q", status.Convert(err).Message())
	}
}

// A pool that exists but whose nodes are over capacity is refused with the
// pool named too, and never falls back to the shared nodes.
func TestSchedulePoolRefusalNamesPoolWhenNoNodeAdmits(t *testing.T) {
	service, _ := newPoolService(t, &config.NodeResourceLimit{MaxSandboxCount: uint32Ptr(0)})
	heartbeatWithPool(t, service, "shared", "", 1)
	heartbeatWithPool(t, service, "spot-a", "spot", 1)

	_, err := service.Schedule(context.Background(), newSandboxRequest("spot"))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("expected unavailable, got %v", err)
	}
	if !strings.Contains(status.Convert(err).Message(), "spot") {
		t.Fatalf("expected the refusal to name the pool, got %q", status.Convert(err).Message())
	}
}

// With no pool configured anywhere, node selection is exactly today's: every
// discovered node is a candidate.
func TestScheduleWithoutPoolKeepsTodayBehavior(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{
		{ID: "node-a", Endpoint: "http://node-a"},
		{ID: "node-b", Endpoint: "http://node-b"},
	}, defaultObservedReportTTL)
	service := NewService(
		zap.NewNop(),
		registry,
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
	)
	heartbeatWithPool(t, service, "node-a", "", 0)
	heartbeatWithPool(t, service, "node-b", "", 0)

	seen := map[string]bool{}
	for i := 0; i < 4; i++ {
		resp, err := service.Schedule(context.Background(), newSandboxRequest(""))
		if err != nil {
			t.Fatalf("schedule failed: %v", err)
		}
		seen[resp.GetNode().GetNodeId()] = true
	}
	if !seen["node-a"] || !seen["node-b"] {
		t.Fatalf("expected both nodes to be candidates, saw %v", seen)
	}
}

// The heartbeat's pool is stored and surfaced by /nodes (ListObservedNodes).
func TestObservedNodeCarriesReportedPool(t *testing.T) {
	service, _ := newPoolService(t, nil)
	heartbeatWithPool(t, service, "spot-a", "spot", 0)
	heartbeatWithPool(t, service, "shared", "", 0)

	resp, err := service.ListObservedNodes(context.Background(), &schedulerv1.ListObservedNodesRequest{ClusterId: "cluster-1"})
	if err != nil {
		t.Fatalf("list observed nodes failed: %v", err)
	}
	got := map[string]string{}
	for _, node := range resp.GetNodes() {
		got[node.GetNodeId()] = node.GetPool()
	}
	if got["spot-a"] != "spot" {
		t.Fatalf("expected spot-a pool %q, got %q", "spot", got["spot-a"])
	}
	if got["shared"] != "" {
		t.Fatalf("expected shared node pool empty, got %q", got["shared"])
	}
}

// A node whose heartbeat never reported a pool is a shared node, so a request
// with a pool does not fall back to it while the node is still registering.
func TestNodeWithoutPoolHeartbeatIsShared(t *testing.T) {
	registry := NewAtomicNodeRegistry([]Node{{ID: "fresh", Endpoint: "http://fresh"}}, defaultObservedReportTTL)
	if got := registry.NodePool("fresh"); got != "" {
		t.Fatalf("expected empty pool before heartbeat, got %q", got)
	}
}

// heartbeatWithPoolAndPaused registers a node in a pool with one paused
// sandbox bound to it, the shape a resume decision projects.
func heartbeatWithPoolAndPaused(
	t *testing.T,
	service *Service,
	nodeID string,
	pool string,
	sandboxID string,
	allocatedGiB uint64,
	pausedMemoryGiB uint64,
) {
	t.Helper()
	_, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            nodeID,
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-" + nodeID,
		Pool:              pool,
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

// A resume whose owner is outside the requested pool is deferred, and the
// reason names the pool instead of silently resuming on another node.
func TestScheduleResumeDefersWhenOwnerIsOutsideThePool(t *testing.T) {
	service, _ := newResumeService(t, twoNodesCapacity(), admissionLimit(false))
	heartbeatWithPoolAndPaused(t, service, "owner", "spot", "sbx-1", 10, 16)
	heartbeatWithPoolAndPaused(t, service, "alt", "", "", 10, 0)

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{
		SandboxId: "sbx-1",
		CpuCount:  4,
		Pool:      "",
	})
	if err != nil {
		t.Fatalf("schedule resume failed: %v", err)
	}
	if resp.GetAdmitted() {
		t.Fatalf("expected the resume outside the pool to be deferred, got %+v", resp)
	}
	if !strings.Contains(resp.GetReason(), "pool") {
		t.Fatalf("expected the reason to name the pool, got %q", resp.GetReason())
	}
}

// Cross-node resume moves a full owner's sandbox only to a node of the
// requested pool.
func TestScheduleResumeMovesWithinRequestedPool(t *testing.T) {
	service, _ := newResumeService(t, twoNodesCapacity(), admissionLimit(true))
	heartbeatWithPoolAndPaused(t, service, "owner", "spot", "sbx-1", 90, 16)
	heartbeatWithPoolAndPaused(t, service, "alt", "spot", "", 10, 0)

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{
		SandboxId: "sbx-1",
		CpuCount:  4,
		Pool:      "spot",
	})
	if err != nil {
		t.Fatalf("schedule resume failed: %v", err)
	}
	if !resp.GetAdmitted() {
		t.Fatalf("expected the resume to move within the pool, got %+v", resp)
	}
	if got := resp.GetNode().GetNodeId(); got != "alt" {
		t.Fatalf("expected alt, got %q", got)
	}
}

// A node of another pool is not a fallback: the resume is deferred and the
// reason names the requested pool.
func TestScheduleResumeDoesNotMoveOutsideRequestedPool(t *testing.T) {
	service, _ := newResumeService(t, twoNodesCapacity(), admissionLimit(true))
	heartbeatWithPoolAndPaused(t, service, "owner", "spot", "sbx-1", 90, 16)
	heartbeatWithPoolAndPaused(t, service, "alt", "", "", 10, 0)

	resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{
		SandboxId: "sbx-1",
		CpuCount:  4,
		Pool:      "spot",
	})
	if err != nil {
		t.Fatalf("schedule resume failed: %v", err)
	}
	if resp.GetAdmitted() {
		t.Fatalf("expected no fallback outside the pool, got %+v", resp)
	}
	if !strings.Contains(resp.GetReason(), "spot") {
		t.Fatalf("expected the reason to name the pool, got %q", resp.GetReason())
	}
}
