package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

// A 16 GiB paused shape cached when the sandbox was created is sent to the
// scheduler so the resume is evaluated with the template shape.
func TestResumeSendsCachedTemplateShapeToScheduler(t *testing.T) {
	var got *schedulerv1.ScheduleResumeRequest
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleResumeFunc: func(_ context.Context, req *schedulerv1.ScheduleResumeRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResumeResponse, error) {
			got = req
			return &schedulerv1.ScheduleResumeResponse{
				Node:     &schedulerv1.Node{NodeId: "owner", Endpoint: upstream.URL},
				Admitted: true,
			}, nil
		},
	}, 5*time.Second, 4<<20)
	server.sandboxShapes.set("sbx-1", sandboxShape{CPUCount: 4, MemoryBytes: 16 << 30})

	request := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes/sbx-1/resume?clusterID=cluster-1", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected proxy status 200, got %d: %s", response.Code, response.Body.String())
	}
	if got == nil {
		t.Fatal("expected a ScheduleResume call")
	}
	if got.GetSandboxId() != "sbx-1" || got.GetClusterId() != "cluster-1" {
		t.Fatalf("unexpected identity: %+v", got)
	}
	if got.GetCpuCount() != 4 || got.GetMemoryBytes() != 16<<30 {
		t.Fatalf("cached shape not forwarded: %+v", got)
	}
}

// An admitted resume is proxied to the node the scheduler returned (which may
// differ from the old owner) and the gateway does not rewrite the binding: the
// scheduler already did, only after selecting.
func TestResumeProxiesToAdmittedNodeWithoutRecordingAssignment(t *testing.T) {
	var upstreamPath string
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamPath = r.URL.Path
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	recordCalls := 0
	server := newTestServer(t, stubSchedulerClient{
		scheduleResumeFunc: func(_ context.Context, _ *schedulerv1.ScheduleResumeRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResumeResponse, error) {
			return &schedulerv1.ScheduleResumeResponse{
				Node:     &schedulerv1.Node{NodeId: "alt", Endpoint: upstream.URL},
				Admitted: true,
			}, nil
		},
		recordAssignmentFunc: func(_ context.Context, _ *schedulerv1.RecordAssignmentRequest, _ ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
			recordCalls++
			return &schedulerv1.RecordAssignmentResponse{}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes/sbx-1/resume", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	if upstreamPath != "/sandboxes/sbx-1/resume" {
		t.Fatalf("upstream path = %q", upstreamPath)
	}
	if recordCalls != 0 {
		t.Fatalf("the gateway must not rewrite a resume binding, got %d calls", recordCalls)
	}
}

// A deferred resume is a retryable 503 with Retry-After and the stable code
// plus the closed reason.
func TestResumeDeferredReturns503WithRetryAfterAndCode(t *testing.T) {
	upstreamHit := false
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamHit = true
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleResumeFunc: func(_ context.Context, _ *schedulerv1.ScheduleResumeRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResumeResponse, error) {
			return &schedulerv1.ScheduleResumeResponse{
				Node:     &schedulerv1.Node{NodeId: "owner", Endpoint: upstream.URL},
				Admitted: false,
				Reason:   "memory_allocated",
			}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes/sbx-1/resume", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", response.Code)
	}
	if got := response.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}
	var body struct {
		Code   string `json:"code"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Code != "sandbox_capacity_deferred" || body.Reason != "memory_allocated" {
		t.Fatalf("unexpected body: %+v", body)
	}
	if upstreamHit {
		t.Fatal("a deferred resume must not reach the node")
	}
}

// An unknown shape defers with no reason instead of inventing one outside the
// closed set.
func TestResumeDeferredOmitsUnknownReason(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		scheduleResumeFunc: func(_ context.Context, _ *schedulerv1.ScheduleResumeRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResumeResponse, error) {
			return &schedulerv1.ScheduleResumeResponse{
				Node:     &schedulerv1.Node{NodeId: "owner", Endpoint: "http://owner"},
				Admitted: false,
			}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes/sbx-1/resume", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", response.Code)
	}
	body, _ := io.ReadAll(response.Body)
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if _, ok := decoded["reason"]; ok {
		t.Fatalf("reason must be omitted when unknown, got %s", body)
	}
}

func TestSandboxShapeCacheDropsEmptyShape(t *testing.T) {
	cache := newSandboxShapeCache()
	cache.set("sbx-1", sandboxShape{MemoryBytes: 0})
	if _, ok := cache.get("sbx-1"); ok {
		t.Fatal("a shape without memory must not be cached")
	}
	cache.set("sbx-1", sandboxShape{MemoryBytes: 8 << 30})
	if _, ok := cache.get("sbx-1"); !ok {
		t.Fatal("expected the shape to be cached")
	}
	cache.forget("sbx-1")
	if _, ok := cache.get("sbx-1"); ok {
		t.Fatal("expected forget to drop the shape")
	}
}

// The resume body may ask for a pool; the gateway forwards it so the scheduler
// keeps the sandbox on nodes of that pool.
func TestResumeForwardsPoolToScheduler(t *testing.T) {
	var got *schedulerv1.ScheduleResumeRequest
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()

	server := newTestServer(t, stubSchedulerClient{
		scheduleResumeFunc: func(_ context.Context, req *schedulerv1.ScheduleResumeRequest, _ ...grpc.CallOption) (*schedulerv1.ScheduleResumeResponse, error) {
			got = req
			return &schedulerv1.ScheduleResumeResponse{
				Node:     &schedulerv1.Node{NodeId: "owner", Endpoint: upstream.URL},
				Admitted: true,
			}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes/sbx-1/resume", strings.NewReader(`{"timeout":60,"pool":"spot"}`))
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	if got == nil {
		t.Fatal("expected a ScheduleResume call")
	}
	if got.GetPool() != "spot" {
		t.Fatalf("pool = %q, want spot", got.GetPool())
	}
}
