package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/admission"
	"agentenv/services/shared/config"

	"google.golang.org/grpc"
)

func withNodeResourceLimit(limit *config.NodeResourceLimit) testServerOption {
	return func(options *ServerOptions) {
		options.NodeResourceLimit = limit
	}
}

func decodeSingleNode(t *testing.T, body []byte) map[string]any {
	t.Helper()
	var nodes []map[string]any
	if err := json.Unmarshal(body, &nodes); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(nodes) != 1 {
		t.Fatalf("expected one node, got %d", len(nodes))
	}
	return nodes[0]
}

func observedNodeWithSnapshot(snapshot *schedulerv1.NodeSnapshot) stubSchedulerClient {
	return stubSchedulerClient{
		listObservedFunc: func(_ context.Context, _ *schedulerv1.ListObservedNodesRequest, _ ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return &schedulerv1.ListObservedNodesResponse{Nodes: []*schedulerv1.ObservedNode{{
				NodeId:    "z9d4",
				Snapshot:  snapshot,
				Admission: &schedulerv1.NodeAdmission{},
			}}}, nil
		},
	}
}

// /nodes publishes the per-node ceiling contract the ProductFactory ceiling
// reader recognises: admission.contract = per_node_ceiling_v1 plus the
// including-paused figures computed from the same reading the scheduler uses.
// Without an explicit including-paused CPU cap the memory ceiling is published
// and the CPU figure is omitted, because paused sandboxes do not consume CPU.
func TestNodeListPublishesMemoryCeilingWithoutDerivedCPUCeiling(t *testing.T) {
	margin := uint64(8) << 30
	percent := uint32(200)
	server := newTestServer(t, observedNodeWithSnapshot(&schedulerv1.NodeSnapshot{
		Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
		MemoryTotalBytes: 120925978624,
		CpuCount:         14,
	}), 5*time.Second, 4<<20, withNodeResourceLimit(&config.NodeResourceLimit{
		MaxAllocatedMemoryMarginBytes: &margin,
		MaxCPUAllocatedPercent:        &percent,
	}))

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}

	node := decodeSingleNode(t, response.Body.Bytes())
	admissionObject, ok := node["admission"].(map[string]any)
	if !ok {
		t.Fatalf("missing admission object: %v", node)
	}
	if got := admissionObject["contract"]; got != admission.ContractPerNodeCeilingV1 {
		t.Fatalf("contract = %v, want %q", got, admission.ContractPerNodeCeilingV1)
	}
	ceilings, ok := admissionObject["ceilings"].(map[string]any)
	if !ok {
		t.Fatalf("missing ceilings object: %v", admissionObject)
	}
	if got := ceilings["memoryBytes"]; got != float64(112336044032) {
		t.Fatalf("ceilings.memoryBytes = %v", got)
	}
	if _, ok := ceilings["cpuCores"]; ok {
		t.Fatalf("cpuCores must be omitted without the explicit cap: %v", ceilings)
	}
	// A known ceiling carries no reason: the field is written only when no
	// ceiling could be computed.
	if _, ok := admissionObject["contractReason"]; ok {
		t.Fatalf("contractReason must be absent when the memory ceiling is known: %v", admissionObject)
	}
}

// The explicit including-paused CPU cap is published verbatim beside the memory
// ceiling.
func TestNodeListPublishesExplicitIncludingPausedCPUCap(t *testing.T) {
	margin := uint64(8) << 30
	percent := uint32(200)
	cap := uint32(20)
	server := newTestServer(t, observedNodeWithSnapshot(&schedulerv1.NodeSnapshot{
		Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
		MemoryTotalBytes: 120925978624,
		CpuCount:         14,
	}), 5*time.Second, 4<<20, withNodeResourceLimit(&config.NodeResourceLimit{
		MaxAllocatedMemoryMarginBytes:  &margin,
		MaxCPUAllocatedPercent:         &percent,
		MaxAllocatedCPUIncludingPaused: &cap,
	}))

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}

	node := decodeSingleNode(t, response.Body.Bytes())
	admissionObject, _ := node["admission"].(map[string]any)
	ceilings, ok := admissionObject["ceilings"].(map[string]any)
	if !ok {
		t.Fatalf("missing ceilings object: %v", admissionObject)
	}
	if got := ceilings["cpuCores"]; got != float64(20) {
		t.Fatalf("ceilings.cpuCores = %v, want 20", got)
	}
	if got := ceilings["memoryBytes"]; got != float64(112336044032) {
		t.Fatalf("ceilings.memoryBytes = %v", got)
	}
}

// Without the margin key the payload keeps the historical shape: no contract
// marker, so the ceiling reader sees the legacy path and never a per-node
// ceiling.
func TestNodeListOmitsNodeCeilingContractWithoutMarginKey(t *testing.T) {
	percent := uint32(200)
	server := newTestServer(t, observedNodeWithSnapshot(&schedulerv1.NodeSnapshot{
		Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
		MemoryTotalBytes: 120925978624,
		CpuCount:         14,
	}), 5*time.Second, 4<<20, withNodeResourceLimit(&config.NodeResourceLimit{
		MaxCPUAllocatedPercent: &percent,
	}))

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)

	node := decodeSingleNode(t, response.Body.Bytes())
	admissionObject, _ := node["admission"].(map[string]any)
	if _, ok := admissionObject["contract"]; ok {
		t.Fatalf("contract must be absent without the margin key: %v", admissionObject)
	}
}

// The margin key declared with a reading that cannot produce the ceilings still
// publishes the marker, with the named reason riding it: the scheduler refuses
// this same node with capacity_unknown, and publishing nothing would let the
// gateway state drift from the decision and look like the legacy path.
func TestNodeListPublishesContractWithCapacityUnknownOnUnknownReading(t *testing.T) {
	margin := uint64(8) << 30
	percent := uint32(200)
	server := newTestServer(t, observedNodeWithSnapshot(&schedulerv1.NodeSnapshot{
		Status:   schedulerv1.NodeStatus_NODE_STATUS_READY,
		CpuCount: 14,
	}), 5*time.Second, 4<<20, withNodeResourceLimit(&config.NodeResourceLimit{
		MaxAllocatedMemoryMarginBytes: &margin,
		MaxCPUAllocatedPercent:        &percent,
	}))

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}

	node := decodeSingleNode(t, response.Body.Bytes())
	admissionObject, ok := node["admission"].(map[string]any)
	if !ok {
		t.Fatalf("missing admission object: %v", node)
	}
	if got := admissionObject["contract"]; got != admission.ContractPerNodeCeilingV1 {
		t.Fatalf("contract = %v, want %q", got, admission.ContractPerNodeCeilingV1)
	}
	if _, ok := admissionObject["ceilings"]; ok {
		t.Fatalf("ceilings must be absent when the reading cannot produce them: %v", admissionObject)
	}
	if got := admissionObject["contractReason"]; got != admission.ReasonCapacityUnknown {
		t.Fatalf("contractReason = %v, want %q", got, admission.ReasonCapacityUnknown)
	}
}

// A readable memory total with no configured CPU ceiling still publishes the
// memory ceiling: a missing CPU cap is not an unknown capacity, so the contract
// carries the memory figure and omits only the CPU one.
func TestNodeListPublishesMemoryCeilingWhenCPUCeilingUnconfigured(t *testing.T) {
	margin := uint64(8) << 30
	server := newTestServer(t, observedNodeWithSnapshot(&schedulerv1.NodeSnapshot{
		Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
		MemoryTotalBytes: 120925978624,
		CpuCount:         14,
	}), 5*time.Second, 4<<20, withNodeResourceLimit(&config.NodeResourceLimit{
		MaxAllocatedMemoryMarginBytes: &margin,
	}))

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}

	node := decodeSingleNode(t, response.Body.Bytes())
	admissionObject, ok := node["admission"].(map[string]any)
	if !ok {
		t.Fatalf("missing admission object: %v", node)
	}
	if got := admissionObject["contract"]; got != admission.ContractPerNodeCeilingV1 {
		t.Fatalf("contract = %v, want %q", got, admission.ContractPerNodeCeilingV1)
	}
	ceilings, ok := admissionObject["ceilings"].(map[string]any)
	if !ok {
		t.Fatalf("missing ceilings object: %v", admissionObject)
	}
	if got := ceilings["memoryBytes"]; got != float64(112336044032) {
		t.Fatalf("ceilings.memoryBytes = %v", got)
	}
	if _, ok := ceilings["cpuCores"]; ok {
		t.Fatalf("cpuCores must be omitted without the explicit cap: %v", ceilings)
	}
	if _, ok := admissionObject["contractReason"]; ok {
		t.Fatalf("contractReason must be absent when the memory ceiling is known: %v", admissionObject)
	}
}
