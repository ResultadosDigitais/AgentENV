package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func listNodeMetrics(t *testing.T, snapshots ...*schedulerv1.NodeSnapshot) []map[string]any {
	t.Helper()
	observed := make([]*schedulerv1.ObservedNode, 0, len(snapshots))
	for _, snapshot := range snapshots {
		observed = append(observed, &schedulerv1.ObservedNode{NodeId: "node", Snapshot: snapshot})
	}
	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(_ context.Context, _ *schedulerv1.ListObservedNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return &schedulerv1.ListObservedNodesResponse{Nodes: observed}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}

	var nodes []map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &nodes); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	metrics := make([]map[string]any, 0, len(nodes))
	for _, node := range nodes {
		metrics = append(metrics, node["metrics"].(map[string]any))
	}
	return metrics
}

// /nodes publishes the working set next to the raw figure, and omits it for a
// node that does not report it instead of publishing a zero.
func TestNodeListPublishesMemoryWorkingSetOnlyWhenReported(t *testing.T) {
	metrics := listNodeMetrics(t,
		&schedulerv1.NodeSnapshot{
			MemoryUsedBytes:       250000,
			MemoryWorkingSetBytes: proto.Uint64(200000),
		},
		&schedulerv1.NodeSnapshot{MemoryUsedBytes: 250000},
	)

	if got := metrics[0]["memoryWorkingSetBytes"]; got != float64(200000) {
		t.Fatalf("memoryWorkingSetBytes = %v, want 200000", got)
	}
	if got := metrics[0]["memoryUsedBytes"]; got != float64(250000) {
		t.Fatalf("memoryUsedBytes must stay raw, got %v", got)
	}
	if got, ok := metrics[1]["memoryWorkingSetBytes"]; ok {
		t.Fatalf("absent working set must be omitted, got %v", got)
	}
}
