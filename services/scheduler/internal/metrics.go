package scheduler

import (
	"context"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/observability"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
)

var (
	schedulerRPCDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_scheduler_rpc_duration_seconds",
			Help:    "Scheduler gRPC duration by RPC and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"rpc", "status"},
	)
	schedulerScheduleDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "agentenv_scheduler_schedule_duration_seconds",
			Help:    "Scheduler Schedule duration by strategy and status.",
			Buckets: observability.DurationBuckets,
		},
		[]string{"strategy", "status"},
	)
	schedulerScheduleAssignments = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_schedule_assignments_total",
			Help: "Successful scheduler assignments by strategy.",
		},
		[]string{"strategy"},
	)
	schedulerAdmissionShadow = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_admission_shadow_total",
			Help: "Per-node memory admission comparisons in shadow mode by outcome (agree, request_would_reject, request_would_admit).",
		},
		[]string{"outcome"},
	)
	schedulerDiskAdmission = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_disk_admission_total",
			Help: "Per-node disk admission outcomes by outcome (within_limit, over_limit, mount_missing). Counted once per node with a heartbeat.",
		},
		[]string{"outcome"},
	)
	schedulerObservedNodes = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "agentenv_scheduler_observed_nodes",
			Help: "Observed node count by derived status.",
		},
		[]string{"status"},
	)
	schedulerCapacitySource = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_capacity_source_total",
			Help: "Per-node admission memory base by source (allocatable, total). Counted once per node per admission evaluation.",
		},
		[]string{"source"},
	)
	schedulerNodeAdmission = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_node_admission_total",
			Help: "Published per-layer admission verdicts by layer, admitted and reason (none when admitted).",
		},
		[]string{"layer", "admitted", "reason"},
	)
	schedulerResumeAdmission = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_resume_admission_total",
			Help: "Resume admission outcomes by outcome (owner, moved, deferred, disabled, error).",
		},
		[]string{"outcome"},
	)
	schedulerAdmissionReservation = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_admission_reservation_total",
			Help: "Admission reservations by outcome (reserved, confirmed, settled, released, expired).",
		},
		[]string{"outcome"},
	)
	schedulerShapeAdmission = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "agentenv_scheduler_shape_admission_total",
			Help: "Shape admission verdicts by kind (create, resume), outcome (admit, would_reject, would_reject_all, would_defer, would_move, unknown_shape) and reason.",
		},
		[]string{"kind", "outcome", "reason"},
	)
)

// Shape admission kinds, outcomes and reasons. The closed sets keep the
// metric label cardinality bounded and make every consumer switch exhaustive.
const (
	shapeKindCreate = "create"
	shapeKindResume = "resume"

	shapeOutcomeAdmit          = "admit"
	shapeOutcomeWouldReject    = "would_reject"
	shapeOutcomeWouldRejectAll = "would_reject_all"
	shapeOutcomeWouldDefer     = "would_defer"
	shapeOutcomeWouldMove      = "would_move"
	shapeOutcomeUnknownShape   = "unknown_shape"

	shapeReasonNone                 = "none"
	shapeReasonUnknownShape         = "unknown_shape"
	shapeReasonUnknownTemplateShape = "unknown_template_shape"
)

func recordShapeAdmission(kind, outcome, reason string) {
	if reason == "" {
		reason = shapeReasonNone
	}
	schedulerShapeAdmission.WithLabelValues(kind, outcome, reason).Inc()
}

func recordAdmissionReservation(outcome string) {
	schedulerAdmissionReservation.WithLabelValues(outcome).Inc()
}

func recordAdmissionReservationCount(outcome string, count int) {
	if count > 0 {
		schedulerAdmissionReservation.WithLabelValues(outcome).Add(float64(count))
	}
}

func MetricsUnaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		rpc := schedulerRPCLabel(info.FullMethod)
		if rpc == "" {
			return handler(ctx, req)
		}
		start := time.Now()
		resp, err := handler(ctx, req)
		recordSchedulerRPC(rpc, start, err)
		return resp, err
	}
}

func recordSchedulerRPC(rpc string, start time.Time, err error) {
	status := observability.GRPCStatusLabel(err)
	schedulerRPCDuration.WithLabelValues(rpc, status).Observe(time.Since(start).Seconds())
}

func recordSchedulerSchedule(strategy string, start time.Time, err error) {
	strategy = schedulerStrategyLabel(strategy)
	status := observability.GRPCStatusLabel(err)
	schedulerScheduleDuration.WithLabelValues(strategy, status).Observe(time.Since(start).Seconds())
	if err == nil {
		schedulerScheduleAssignments.WithLabelValues(strategy).Inc()
	}
}

func recordObservedNodes(nodes []*schedulerv1.ObservedNode) {
	counts := map[string]int{
		"ready":       0,
		"connecting":  0,
		"unhealthy":   0,
		"lingering":   0,
		"unspecified": 0,
	}
	for _, node := range nodes {
		counts[schedulerNodeStatusLabel(node.GetSnapshot().GetStatus())]++
	}
	for label, count := range counts {
		schedulerObservedNodes.WithLabelValues(label).Set(float64(count))
	}
}

func schedulerRPCLabel(fullMethod string) string {
	switch fullMethod[strings.LastIndex(fullMethod, "/")+1:] {
	case "Schedule":
		return "Schedule"
	case "ListNodes":
		return "ListNodes"
	case "LookupNode":
		return "LookupNode"
	case "RecordAssignment":
		return "RecordAssignment"
	case "Heartbeat":
		return "Heartbeat"
	case "ListObservedNodes":
		return "ListObservedNodes"
	case "ReportSandboxEvent":
		return "ReportSandboxEvent"
	case "GetNode":
		return "GetNode"
	case "UnregisterNode":
		return "UnregisterNode"
	case "ScheduleResume":
		return "ScheduleResume"
	case "SettleAdmission":
		return "SettleAdmission"
	default:
		return ""
	}
}

func schedulerStrategyLabel(strategy string) string {
	switch strings.ToLower(strings.TrimSpace(strategy)) {
	case "round_robin":
		return "round_robin"
	case "random":
		return "random"
	case "least_loaded":
		return "least_loaded"
	default:
		return "unknown"
	}
}

func schedulerNodeStatusLabel(status schedulerv1.NodeStatus) string {
	switch status {
	case schedulerv1.NodeStatus_NODE_STATUS_READY:
		return "ready"
	case schedulerv1.NodeStatus_NODE_STATUS_CONNECTING:
		return "connecting"
	case schedulerv1.NodeStatus_NODE_STATUS_UNHEALTHY:
		return "unhealthy"
	case schedulerv1.NodeStatus_NODE_STATUS_LINGERING:
		return "lingering"
	default:
		return "unspecified"
	}
}
