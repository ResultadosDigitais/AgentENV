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
)

// /nodes publishes the scheduler's admission verdict, with the capacity audit
// fields and the closed reason only on rejected layers.
func TestNodeListPublishesAdmissionVerdicts(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(_ context.Context, _ *schedulerv1.ListObservedNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return &schedulerv1.ListObservedNodesResponse{Nodes: []*schedulerv1.ObservedNode{{
				NodeId: "z9d4",
				Snapshot: &schedulerv1.NodeSnapshot{
					Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
					MemoryTotalBytes: 128819 << 20,
				},
				Admission: &schedulerv1.NodeAdmission{
					AllocatableMemoryBytes: 119336 << 20,
					RuntimeOverheadBytes:   4000 << 20,
					CapacityMemoryBytes:    115336 << 20,
					CapacitySource:         "allocatable",
					Layers: []*schedulerv1.AdmissionVerdict{
						{MemoryGb: 8, Admitted: true},
						{MemoryGb: 16, Admitted: false, Reason: "memory_allocated"},
					},
				},
			}}}, nil
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
	admission, ok := nodes[0]["admission"].(map[string]any)
	if !ok {
		t.Fatalf("missing admission object: %v", nodes[0])
	}
	if got := admission["capacitySource"]; got != "allocatable" {
		t.Fatalf("capacitySource = %v", got)
	}
	if got := admission["capacityMemoryBytes"]; got != float64(115336<<20) {
		t.Fatalf("capacityMemoryBytes = %v", got)
	}
	layers, ok := admission["layers"].([]any)
	if !ok || len(layers) != 2 {
		t.Fatalf("unexpected layers: %v", admission["layers"])
	}
	admitted := layers[0].(map[string]any)
	if _, ok := admitted["reason"]; ok {
		t.Fatalf("an admitted layer must omit reason: %v", admitted)
	}
	rejected := layers[1].(map[string]any)
	if got := rejected["reason"]; got != "memory_allocated" {
		t.Fatalf("rejected reason = %v", got)
	}
}

// A scheduler without verdicts (kill switch off) must omit the admission
// object instead of publishing an empty one.
func TestNodeListOmitsAdmissionWhenSchedulerHasNone(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(_ context.Context, _ *schedulerv1.ListObservedNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return &schedulerv1.ListObservedNodesResponse{Nodes: []*schedulerv1.ObservedNode{{
				NodeId:   "node-a",
				Snapshot: &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
			}}}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)

	var nodes []map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &nodes); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if _, ok := nodes[0]["admission"]; ok {
		t.Fatalf("admission must be omitted, got %v", nodes[0]["admission"])
	}
}

// /nodes publishes the pool the node agent reported, empty for a shared node.
func TestNodeListPublishesNodePool(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(_ context.Context, _ *schedulerv1.ListObservedNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return &schedulerv1.ListObservedNodesResponse{Nodes: []*schedulerv1.ObservedNode{
				{
					NodeId:   "spot-a",
					Pool:     "spot",
					Snapshot: &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
				},
				{
					NodeId:   "shared",
					Snapshot: &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
				},
			}}, nil
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
	byID := map[string]map[string]any{}
	for _, node := range nodes {
		byID[node["id"].(string)] = node
	}
	if got := byID["spot-a"]["pool"]; got != "spot" {
		t.Fatalf("spot-a pool = %v, want spot", got)
	}
	if got, ok := byID["shared"]["pool"]; !ok || got != "" {
		t.Fatalf("shared pool = %v (present=%v), want empty string", got, ok)
	}
}
