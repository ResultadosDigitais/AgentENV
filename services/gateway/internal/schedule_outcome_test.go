package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"google.golang.org/grpc"
)

// SettleAdmission is a no-op for every stub that does not record settlements.
func (s stubSchedulerClient) SettleAdmission(context.Context, *schedulerv1.SettleAdmissionRequest, ...grpc.CallOption) (*schedulerv1.SettleAdmissionResponse, error) {
	return &schedulerv1.SettleAdmissionResponse{}, nil
}

// settleRecorder wraps a stub and records every SettleAdmission call.
type settleRecorder struct {
	stubSchedulerClient
	mu    sync.Mutex
	calls []*schedulerv1.SettleAdmissionRequest
	err   error
}

func (r *settleRecorder) SettleAdmission(_ context.Context, req *schedulerv1.SettleAdmissionRequest, _ ...grpc.CallOption) (*schedulerv1.SettleAdmissionResponse, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, req)
	if r.err != nil {
		return nil, r.err
	}
	return &schedulerv1.SettleAdmissionResponse{}, nil
}

func (r *settleRecorder) settled() []*schedulerv1.SettleAdmissionRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*schedulerv1.SettleAdmissionRequest(nil), r.calls...)
}

func scheduleTo(endpoint, reservationID string) func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
	return func(context.Context, *schedulerv1.ScheduleRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResponse, error) {
		return &schedulerv1.ScheduleResponse{
			Node:          &schedulerv1.Node{NodeId: "node-1", Endpoint: endpoint},
			ReservationId: reservationID,
		}, nil
	}
}

func postCreate(t *testing.T, server *Server) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes", strings.NewReader(`{"templateID":"base"}`))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)
	return response
}

func assertSettledOnce(t *testing.T, recorder *settleRecorder, reservationID, outcome string) {
	t.Helper()
	calls := recorder.settled()
	if len(calls) != 1 {
		t.Fatalf("SettleAdmission calls = %d (%v), want exactly 1", len(calls), calls)
	}
	if calls[0].GetReservationId() != reservationID || calls[0].GetOutcome() != outcome {
		t.Fatalf("settled %q/%q, want %q/%q", calls[0].GetReservationId(), calls[0].GetOutcome(), reservationID, outcome)
	}
}

func TestCreateSettlesReservationWithUpstreamOutcome(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		outcome string
	}{
		{"created", http.StatusCreated, reservationOutcomeConfirmed},
		{"rejected", http.StatusInternalServerError, reservationOutcomeFailed},
		{"client error", http.StatusBadRequest, reservationOutcomeFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(`{"sandboxID":"sbx-1"}`))
			}))
			defer upstream.Close()
			recorder := &settleRecorder{stubSchedulerClient: stubSchedulerClient{
				scheduleFunc: scheduleTo(upstream.URL, "reservation-7"),
				recordAssignmentFunc: func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
					return &schedulerv1.RecordAssignmentResponse{}, nil
				},
			}}
			server := newTestServer(t, recorder, 5*time.Second, 4<<20)

			if response := postCreate(t, server); response.Code != tc.status {
				t.Fatalf("status = %d, want %d", response.Code, tc.status)
			}
			assertSettledOnce(t, recorder, "reservation-7", tc.outcome)
		})
	}
}

// A request that never reaches the upstream releases its reservation.
func TestCreateSettlesReservationOnTransportError(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := upstream.URL
	upstream.Close()
	recorder := &settleRecorder{stubSchedulerClient: stubSchedulerClient{scheduleFunc: scheduleTo(endpoint, "reservation-8")}}
	server := newTestServer(t, recorder, 5*time.Second, 4<<20)

	if response := postCreate(t, server); response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", response.Code)
	}
	assertSettledOnce(t, recorder, "reservation-8", reservationOutcomeFailed)
}

// A 2xx whose response handling then fails (body over the forwarding limit)
// goes through ModifyResponse and ErrorHandler; the reservation is settled
// once, as confirmed, because the upstream did create the sandbox.
func TestCreateSettlesOnceWhenResponseHandlingFails(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sandboxID":"sbx-with-a-long-body"}`))
	}))
	defer upstream.Close()
	recorder := &settleRecorder{stubSchedulerClient: stubSchedulerClient{scheduleFunc: scheduleTo(upstream.URL, "reservation-9")}}
	server := newTestServer(t, recorder, 5*time.Second, 8)

	if response := postCreate(t, server); response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", response.Code)
	}
	assertSettledOnce(t, recorder, "reservation-9", reservationOutcomeConfirmed)
}

func TestCreateSettlesReservationOnInvalidEndpoint(t *testing.T) {
	recorder := &settleRecorder{stubSchedulerClient: stubSchedulerClient{scheduleFunc: scheduleTo("not-a-url", "reservation-10")}}
	server := newTestServer(t, recorder, 5*time.Second, 4<<20)

	if response := postCreate(t, server); response.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", response.Code)
	}
	assertSettledOnce(t, recorder, "reservation-10", reservationOutcomeFailed)
}

// Settlement is best-effort: a scheduler error never changes the proxied
// response, and an empty reservation id (ledger off) makes no call at all.
func TestCreateSettlementIsBestEffort(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("x-agentenv-sandbox-id", "sbx-1")
		w.WriteHeader(http.StatusCreated)
	}))
	defer upstream.Close()
	recordOK := func(context.Context, *schedulerv1.RecordAssignmentRequest, ...grpc.CallOption) (*schedulerv1.RecordAssignmentResponse, error) {
		return &schedulerv1.RecordAssignmentResponse{}, nil
	}

	failing := &settleRecorder{
		stubSchedulerClient: stubSchedulerClient{scheduleFunc: scheduleTo(upstream.URL, "reservation-11"), recordAssignmentFunc: recordOK},
		err:                 errors.New("scheduler unavailable"),
	}
	if response := postCreate(t, newTestServer(t, failing, 5*time.Second, 4<<20)); response.Code != http.StatusCreated {
		t.Fatalf("a failed settlement changed the status to %d", response.Code)
	}
	assertSettledOnce(t, failing, "reservation-11", reservationOutcomeConfirmed)

	disabled := &settleRecorder{stubSchedulerClient: stubSchedulerClient{scheduleFunc: scheduleTo(upstream.URL, ""), recordAssignmentFunc: recordOK}}
	if response := postCreate(t, newTestServer(t, disabled, 5*time.Second, 4<<20)); response.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", response.Code)
	}
	if calls := disabled.settled(); len(calls) != 0 {
		t.Fatalf("an empty reservation id must not be settled, got %v", calls)
	}
}

func TestResumeSettlesItsReservation(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer upstream.Close()
	recorder := &settleRecorder{stubSchedulerClient: stubSchedulerClient{
		scheduleResumeFunc: func(context.Context, *schedulerv1.ScheduleResumeRequest, ...grpc.CallOption) (*schedulerv1.ScheduleResumeResponse, error) {
			return &schedulerv1.ScheduleResumeResponse{
				Node:          &schedulerv1.Node{NodeId: "owner", Endpoint: upstream.URL},
				Admitted:      true,
				ReservationId: "reservation-12",
			}, nil
		},
	}}
	server := newTestServer(t, recorder, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodPost, "http://gateway.test/sandboxes/sbx-1/resume", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", response.Code)
	}
	assertSettledOnce(t, recorder, "reservation-12", reservationOutcomeConfirmed)
}

// The create shape in the body reaches the scheduler hint, so the decision
// can reserve memory and CPU instead of a bare slot.
func TestBuildScheduleHintNewSandboxCarriesShape(t *testing.T) {
	r := newHintRequest(t, http.MethodPost, "/sandboxes", `{"templateID":"tmpl","cpuCount":4,"memoryMB":30720}`)
	hint, err := buildScheduleHint(r)
	if err != nil {
		t.Fatalf("buildScheduleHint returned error: %v", err)
	}
	sandbox := hint.GetNewSandbox()
	if sandbox.GetCpuCount() != 4 || sandbox.GetMemoryMb() != 30720 {
		t.Fatalf("shape not parsed: %+v", sandbox)
	}
}

func TestNodeListPublishesReservedTotals(t *testing.T) {
	server := newTestServer(t, stubSchedulerClient{
		listObservedFunc: func(context.Context, *schedulerv1.ListObservedNodesRequest, ...grpc.CallOption) (*schedulerv1.ListObservedNodesResponse, error) {
			return &schedulerv1.ListObservedNodesResponse{Nodes: []*schedulerv1.ObservedNode{{
				NodeId:   "z9d4",
				Snapshot: &schedulerv1.NodeSnapshot{Status: schedulerv1.NodeStatus_NODE_STATUS_READY},
				Admission: &schedulerv1.NodeAdmission{
					CapacitySource:      "allocatable",
					ReservedMemoryBytes: 30 << 30,
					ReservedCpu:         4,
				},
			}}}, nil
		},
	}, 5*time.Second, 4<<20)

	request := httptest.NewRequest(http.MethodGet, "http://gateway.test/nodes", nil)
	response := httptest.NewRecorder()
	authenticatedTestHandler(server).ServeHTTP(response, request)
	var nodes []map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &nodes); err != nil {
		t.Fatalf("decode response: %v (%s)", err, response.Body.String())
	}
	admission := nodes[0]["admission"].(map[string]any)
	if admission["reservedMemoryBytes"] != float64(30<<30) || admission["reservedCPU"] != float64(4) {
		t.Fatalf("reserved totals not published: %v", admission)
	}
}
