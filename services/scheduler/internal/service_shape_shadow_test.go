package scheduler

import (
	"context"
	"testing"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// shapeAdmissionCount reads the shape admission counter straight from
// client_model, following the pattern used by the disk admission tests.
func shapeAdmissionCount(t *testing.T, kind, outcome, reason string) float64 {
	t.Helper()
	counter, ok := schedulerShapeAdmission.WithLabelValues(kind, outcome, reason).(prometheus.Counter)
	if !ok {
		t.Fatalf("shape admission entry %q/%q/%q is not a counter", kind, outcome, reason)
	}
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		t.Fatalf("read shape admission counter: %v", err)
	}
	return metric.GetCounter().GetValue()
}

func resolvedHint(memoryGiB uint64, cpuCount uint32) *schedulerv1.ScheduleRequestHint {
	return &schedulerv1.ScheduleRequestHint{Kind: &schedulerv1.ScheduleRequestHint_NewSandbox{
		NewSandbox: &schedulerv1.NewSandboxHint{
			ResolvedMemoryMb:    memoryGiB << 10,
			ResolvedCpuCount:    cpuCount,
			TemplateShapeStatus: schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_RESOLVED,
			TemplateId:          "tmpl-32g",
		},
	}}
}

func templateStatusHint(status schedulerv1.TemplateShapeStatus) *schedulerv1.ScheduleRequestHint {
	return &schedulerv1.ScheduleRequestHint{Kind: &schedulerv1.ScheduleRequestHint_NewSandbox{
		NewSandbox: &schedulerv1.NewSandboxHint{
			TemplateShapeStatus: status,
			TemplateId:          "tmpl-32g",
		},
	}}
}

// A100 rule: five 32 GiB creates on the OOM pod (112.6 GiB, ceiling 100% over
// memory_total). The third fits at 96 GiB; the fourth needs 128 GiB and is
// refused.
func TestShapeAdmissionA100OOMCase(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(100),
		MemoryCapacitySource:      strPtr(config.MemoryCapacitySourceTotal),
		ShapeAdmission:            strPtr(config.ShapeAdmissionOn),
	}
	service := newReservationService(t, limit, true, nil, "525v")
	total := uint64(1126) * gib / 10 // 112.6 GiB
	heartbeatSnapshot(t, service, "525v", &schedulerv1.NodeSnapshot{MemoryTotalBytes: total, CpuCount: 32}, nil, nil)

	want := []bool{true, true, true, false, false}
	for i, wantAdmitted := range want {
		_, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: coldHint(32, 8)})
		if admitted := err == nil; admitted != wantAdmitted {
			t.Fatalf("create %d admitted = %v (err=%v), want %v", i+1, admitted, err, wantAdmitted)
		}
	}
}

// A create whose body carries no memoryMB projects the resolved template shape
// only with shape_admission on. Shadow records the verdict and keeps today's
// decision; off ignores the resolved shape and records nothing.
func TestShapeAdmissionCreateWithoutMemoryMBUsesResolvedShape(t *testing.T) {
	cases := []struct {
		mode            string
		wantFourthAdmit bool
		wantWouldReject bool
	}{
		{mode: config.ShapeAdmissionOn, wantFourthAdmit: false, wantWouldReject: false},
		{mode: config.ShapeAdmissionShadow, wantFourthAdmit: true, wantWouldReject: true},
		{mode: config.ShapeAdmissionOff, wantFourthAdmit: true, wantWouldReject: false},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			limit := &config.NodeResourceLimit{
				MaxMemoryAllocatedPercent: uint32Ptr(100),
				MemoryCapacitySource:      strPtr(config.MemoryCapacitySourceTotal),
				ShapeAdmission:            strPtr(tc.mode),
			}
			service := newReservationService(t, limit, true, nil, "525v")
			total := uint64(1126) * gib / 10
			// The first three 32 GiB VMs of the OOM case are already allocated;
			// this is the fourth create, whose body carries no memoryMB.
			heartbeatSnapshot(t, service, "525v", &schedulerv1.NodeSnapshot{
				MemoryTotalBytes:     total,
				AllocatedMemoryBytes: 96 * gib,
				MemoryUsedBytes:      96 * gib,
				CpuCount:             32,
			}, nil, nil)

			beforeWouldReject := shapeAdmissionCount(t, shapeKindCreate, shapeOutcomeWouldReject, ReasonMemoryAllocated)
			_, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{Hint: resolvedHint(32, 8)})
			if admitted := err == nil; admitted != tc.wantFourthAdmit {
				t.Fatalf("fourth create admitted = %v (err=%v), want %v", admitted, err, tc.wantFourthAdmit)
			}
			gotWouldReject := shapeAdmissionCount(t, shapeKindCreate, shapeOutcomeWouldReject, ReasonMemoryAllocated) > beforeWouldReject
			if gotWouldReject != tc.wantWouldReject {
				t.Fatalf("would_reject recorded = %v, want %v", gotWouldReject, tc.wantWouldReject)
			}
		})
	}
}

// A shadow resume keeps today's decision: the owner is admitted with no
// reservation and no binding change, and the verdict is recorded.
func TestShapeAdmissionResumeShadowDefersAndMoves(t *testing.T) {
	t.Run("would_defer", func(t *testing.T) {
		limit := admissionLimit(false)
		limit.ShapeAdmission = strPtr(config.ShapeAdmissionShadow)
		service, _ := newResumeService(t, twoNodesCapacity(), limit)
		heartbeatWithPaused(t, service, "owner", "sbx-1", 96, 32)

		before := shapeAdmissionCount(t, shapeKindResume, shapeOutcomeWouldDefer, ReasonMemoryAllocated)
		resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
		if err != nil {
			t.Fatalf("schedule resume failed: %v", err)
		}
		if !resp.GetAdmitted() || resp.GetNode().GetNodeId() != "owner" || resp.GetReservationId() != "" {
			t.Fatalf("shadow must admit the owner without a reservation, got %+v", resp)
		}
		if got := shapeAdmissionCount(t, shapeKindResume, shapeOutcomeWouldDefer, ReasonMemoryAllocated); got <= before {
			t.Fatalf("would_defer not recorded (before=%v after=%v)", before, got)
		}
		if node, ok, _ := service.store.Get("sbx-1", time.Now()); !ok || node.ID != "owner" {
			t.Fatalf("shadow must not move the binding, got %+v ok=%v", node, ok)
		}
	})

	t.Run("would_move", func(t *testing.T) {
		limit := admissionLimit(true)
		limit.ShapeAdmission = strPtr(config.ShapeAdmissionShadow)
		service, _ := newResumeService(t, twoNodesCapacity(), limit)
		heartbeatWithPaused(t, service, "owner", "sbx-1", 96, 32)
		if _, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
			NodeId:            "alt",
			ClusterId:         "cluster-1",
			ServiceInstanceId: "svc-alt",
			Snapshot: &schedulerv1.NodeSnapshot{
				Status:               schedulerv1.NodeStatus_NODE_STATUS_READY,
				MemoryTotalBytes:     100 * gib,
				AllocatedMemoryBytes: 10 * gib,
				MemoryUsedBytes:      10 * gib,
			},
		}); err != nil {
			t.Fatalf("alt heartbeat failed: %v", err)
		}

		before := shapeAdmissionCount(t, shapeKindResume, shapeOutcomeWouldMove, ReasonMemoryAllocated)
		resp, err := service.ScheduleResume(context.Background(), &schedulerv1.ScheduleResumeRequest{SandboxId: "sbx-1"})
		if err != nil {
			t.Fatalf("schedule resume failed: %v", err)
		}
		if !resp.GetAdmitted() || resp.GetNode().GetNodeId() != "owner" {
			t.Fatalf("shadow must keep the owner, got %+v", resp)
		}
		if got := shapeAdmissionCount(t, shapeKindResume, shapeOutcomeWouldMove, ReasonMemoryAllocated); got <= before {
			t.Fatalf("would_move not recorded (before=%v after=%v)", before, got)
		}
	})
}

// A template whose shape could not be read is refused with a typed reason when
// the rule decides and a memory ceiling exists; not_found passes through.
func TestShapeAdmissionUnknownTemplateShape(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(100),
		MemoryCapacitySource:      strPtr(config.MemoryCapacitySourceTotal),
		ShapeAdmission:            strPtr(config.ShapeAdmissionOn),
	}
	service := newReservationService(t, limit, true, nil, "525v")
	heartbeatSnapshot(t, service, "525v", &schedulerv1.NodeSnapshot{MemoryTotalBytes: 100 * gib, CpuCount: 32}, nil, nil)

	resp, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{
		Hint: templateStatusHint(schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED),
	})
	if err != nil {
		t.Fatalf("expected a typed refusal, got error %v", err)
	}
	if got := resp.GetRefusalReason(); got != schedulerv1.ScheduleRefusalReason_SCHEDULE_REFUSAL_REASON_UNKNOWN_TEMPLATE_SHAPE {
		t.Fatalf("refusal_reason = %v, want unknown_template_shape", got)
	}

	resp, err = service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{
		Hint: templateStatusHint(schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_NOT_FOUND),
	})
	if err != nil {
		t.Fatalf("not_found must pass through, got error %v", err)
	}
	if resp.GetNode().GetNodeId() != "525v" || resp.GetRefusalReason() != schedulerv1.ScheduleRefusalReason_SCHEDULE_REFUSAL_REASON_UNSPECIFIED {
		t.Fatalf("not_found must admit the create, got %+v", resp)
	}
}

// An unknown enum value is never admitted blind when the rule decides.
func TestShapeAdmissionUnknownEnumStatusRefuses(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(100),
		MemoryCapacitySource:      strPtr(config.MemoryCapacitySourceTotal),
		ShapeAdmission:            strPtr(config.ShapeAdmissionOn),
	}
	service := newReservationService(t, limit, true, nil, "525v")
	heartbeatSnapshot(t, service, "525v", &schedulerv1.NodeSnapshot{MemoryTotalBytes: 100 * gib, CpuCount: 32}, nil, nil)

	resp, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{
		Hint: templateStatusHint(schedulerv1.TemplateShapeStatus(99)),
	})
	if err != nil {
		t.Fatalf("expected a typed refusal, got error %v", err)
	}
	if got := resp.GetRefusalReason(); got != schedulerv1.ScheduleRefusalReason_SCHEDULE_REFUSAL_REASON_UNKNOWN_TEMPLATE_SHAPE {
		t.Fatalf("refusal_reason = %v, want unknown_template_shape", got)
	}
}
