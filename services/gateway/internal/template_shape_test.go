package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
)

func templateShapeOutcomeCount(t *testing.T, outcome string) float64 {
	t.Helper()
	counter, ok := gatewayTemplateShapeTotal.WithLabelValues(outcome).(prometheus.Counter)
	if !ok {
		t.Fatalf("template shape outcome %q is not a counter", outcome)
	}
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		t.Fatalf("read template shape counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func readyNode(endpoint string) *schedulerv1.ListObservedNodesResponse {
	return &schedulerv1.ListObservedNodesResponse{Nodes: []*schedulerv1.ObservedNode{
		{
			NodeId:   "node-a",
			Endpoint: endpoint,
			Snapshot: &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
		},
	}}
}

func TestResolveTemplateShapeOutcomes(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		body       string
		want       schedulerv1.TemplateShapeStatus
		wantMemory uint64
	}{
		{"resolved", http.StatusOK, `{"builds":[{"cpuCount":8,"memoryMB":32768}]}`, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_RESOLVED, 32768 << 20},
		{"not found", http.StatusNotFound, `{}`, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_NOT_FOUND, 0},
		{"lookup failed", http.StatusInternalServerError, `{}`, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED, 0},
		{"malformed", http.StatusOK, `{not json`, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED, 0},
		{"no build", http.StatusOK, `{"builds":[]}`, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_UNAVAILABLE, 0},
		{"zero memory", http.StatusOK, `{"builds":[{"cpuCount":8,"memoryMB":0}]}`, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_UNAVAILABLE, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/templates/tmpl-32g" {
					t.Errorf("unexpected path %q", r.URL.Path)
				}
				if r.Header.Get(headerAPIKey) != testAPIKey {
					t.Errorf("lookup must carry the gateway API key, got %q", r.Header.Get(headerAPIKey))
				}
				w.WriteHeader(tc.statusCode)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer node.Close()

			server := newTestServer(t, stubSchedulerClient{
				listObservedFunc: func(context.Context, *schedulerv1.ListObservedNodesRequest, ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
					return readyNode(node.URL), nil
				},
			}, time.Second, 1<<20)

			shape, status := server.resolveTemplateShape(context.Background(), "tmpl-32g")
			if status != tc.want {
				t.Fatalf("status = %v, want %v", status, tc.want)
			}
			if shape.MemoryBytes != tc.wantMemory {
				t.Fatalf("memory = %d, want %d", shape.MemoryBytes, tc.wantMemory)
			}
		})
	}
}

func TestResolveTemplateShapeWithoutReadyNode(t *testing.T) {
	before := templateShapeOutcomeCount(t, templateShapeOutcomeNoNode)
	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(context.Context, *schedulerv1.ListObservedNodesRequest, ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return &schedulerv1.ListObservedNodesResponse{Nodes: []*schedulerv1.ObservedNode{
				{
					NodeId:   "node-a",
					Endpoint: "http://unused",
					Snapshot: &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_CONNECTING},
				},
			}}, nil
		},
	}, time.Second, 1<<20)

	_, status := server.resolveTemplateShape(context.Background(), "tmpl-32g")
	if status != schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_UNAVAILABLE {
		t.Fatalf("status = %v, want unavailable", status)
	}
	if got := templateShapeOutcomeCount(t, templateShapeOutcomeNoNode); got <= before {
		t.Fatalf("no_node outcome not recorded (before=%v after=%v)", before, got)
	}
}

func TestTemplateShapeCacheHitAndExpiry(t *testing.T) {
	var reads int
	var mu sync.Mutex
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		reads++
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"builds":[{"cpuCount":8,"memoryMB":32768}]}`))
	}))
	defer node.Close()

	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(context.Context, *schedulerv1.ListObservedNodesRequest, ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return readyNode(node.URL), nil
		},
	}, time.Second, 1<<20)

	now := time.Now()
	server.templateShapes.now = func() time.Time { return now }

	for i := 0; i < 2; i++ {
		if _, status := server.resolveTemplateShape(context.Background(), "tmpl-32g"); status != schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_RESOLVED {
			t.Fatalf("call %d resolved status = %v", i, status)
		}
	}
	if reads != 1 {
		t.Fatalf("warm cache must avoid a second read, got %d reads", reads)
	}

	now = now.Add(templateShapeCacheTTL + time.Second)
	if _, status := server.resolveTemplateShape(context.Background(), "tmpl-32g"); status != schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_RESOLVED {
		t.Fatalf("expired entry status = %v", status)
	}
	if reads != 2 {
		t.Fatalf("expired cache must read again, got %d reads", reads)
	}
}

func TestParseNewSandboxHintExtractsTemplateID(t *testing.T) {
	hint := parseNewSandboxHint([]byte(`{"templateID":"tmpl-32g"}`))
	if got := hint.GetTemplateId(); got != "tmpl-32g" {
		t.Fatalf("template id = %q, want tmpl-32g", got)
	}
}

func TestGatewayCreateUsesResolvedShapeWithoutTouchingBody(t *testing.T) {
	const createBody = `{"templateID":"tmpl-32g"}`

	var mu sync.Mutex
	var templateReads int
	var createBodies []string
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/templates/tmpl-32g":
			mu.Lock()
			templateReads++
			mu.Unlock()
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"builds":[{"cpuCount":8,"memoryMB":32768}]}`))
		case r.Method == http.MethodPost && (r.URL.Path == "/sandboxes" || r.URL.Path == "/v2/sandboxes"):
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			createBodies = append(createBodies, string(body))
			mu.Unlock()
			w.Header().Set(headerSandboxID, "sbx-1")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		default:
			t.Errorf("unexpected node request %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer node.Close()

	var captured *schedulerv1.ScheduleRequestHint
	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(context.Context, *schedulerv1.ListObservedNodesRequest, ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return readyNode(node.URL), nil
		},
		scheduleFunc: func(_ context.Context, req *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			captured = req.GetHint()
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: node.URL}}, nil
		},
		recordAssignmentFunc: func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 5*time.Second, 4<<20)

	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, httptest.NewRequest(
		http.MethodPost, "http://gateway.test/v2/sandboxes", strings.NewReader(createBody),
	))
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}

	sandbox := captured.GetNewSandbox()
	if sandbox == nil {
		t.Fatalf("expected a new sandbox hint, got %v", captured)
	}
	if sandbox.GetResolvedMemoryMb() != 32768 || sandbox.GetResolvedCpuCount() != 8 {
		t.Fatalf("resolved shape = %d MB / %d cpu", sandbox.GetResolvedMemoryMb(), sandbox.GetResolvedCpuCount())
	}
	if sandbox.GetTemplateShapeStatus() != schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_RESOLVED {
		t.Fatalf("template shape status = %v", sandbox.GetTemplateShapeStatus())
	}
	if sandbox.GetTemplateId() != "tmpl-32g" {
		t.Fatalf("template id = %q", sandbox.GetTemplateId())
	}

	mu.Lock()
	defer mu.Unlock()
	if templateReads != 1 {
		t.Fatalf("template reads = %d, want 1", templateReads)
	}
	if len(createBodies) != 1 || createBodies[0] != createBody {
		t.Fatalf("forwarded create body = %v, want the original %q", createBodies, createBody)
	}
	if shape, ok := server.sandboxShapes.get("sbx-1"); !ok || shape.MemoryBytes != 32768<<20 || shape.CPUCount != 8 {
		t.Fatalf("resolved shape must be cached by sandbox id, got %+v ok=%v", shape, ok)
	}
}

func TestGatewayCreateLookupFailureStillReachesNode(t *testing.T) {
	var creates int
	node := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/templates/") {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		creates++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer node.Close()

	var captured *schedulerv1.ScheduleRequestHint
	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(context.Context, *schedulerv1.ListObservedNodesRequest, ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return readyNode(node.URL), nil
		},
		scheduleFunc: func(_ context.Context, req *schedulerv1.ScheduleRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			captured = req.GetHint()
			return &schedulerv1.ScheduleResponse{Node: &schedulerv1.Node{NodeId: "node-a", Endpoint: node.URL}}, nil
		},
	}, 5*time.Second, 4<<20)

	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, httptest.NewRequest(
		http.MethodPost, "http://gateway.test/v2/sandboxes", strings.NewReader(`{"templateID":"tmpl-32g"}`),
	))
	if response.Code != http.StatusOK {
		t.Fatalf("a failed lookup must not block the create, status = %d", response.Code)
	}
	if creates != 1 {
		t.Fatalf("create reached the node %d times, want 1", creates)
	}
	if got := captured.GetNewSandbox().GetTemplateShapeStatus(); got != schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED {
		t.Fatalf("template shape status = %v, want lookup_failed", got)
	}
}

func TestGatewayCreateRefusedOnUnknownTemplateShape(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		scheduleFunc: func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
			return &schedulerv1.ScheduleResponse{
				RefusalReason: schedulerv1.ScheduleRefusalReason_SCHEDULE_REFUSAL_REASON_UNKNOWN_TEMPLATE_SHAPE,
			}, nil
		},
	}, 5*time.Second, 4<<20)

	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, httptest.NewRequest(
		http.MethodPost, "http://gateway.test/v2/sandboxes", strings.NewReader(`{"templateID":"tmpl-32g"}`),
	))
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", response.Code)
	}
	if got := response.Header().Get("Retry-After"); got == "" {
		t.Fatal("a deferred create must carry Retry-After")
	}
	var body struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body failed: %v", err)
	}
	if body.Code != "sandbox_capacity_deferred" || body.Reason != "unknown_template_shape" {
		t.Fatalf("body = %+v", body)
	}
}
