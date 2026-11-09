package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Service struct {
	schedulerv1.UnimplementedSchedulerServer
	logger           *zap.Logger
	nodes            NodeRegistry
	strategy         Strategy
	store            BindingStore
	artifacts        ArtifactStore
	resourceLimit    *config.NodeResourceLimit
	capacityProvider NodeCapacityProvider
	// admissionMu serializes read-decide-reserve so two decisions cannot both
	// observe the same free capacity. It is only taken while reservations are
	// enabled; the ledger keeps its own lock and is always taken inside it.
	admissionMu  sync.Mutex
	reservations *reservationLedger
}

func NewService(logger *zap.Logger, nodes NodeRegistry, strategy Strategy, store BindingStore, opts ...ServiceOption) *Service {
	if logger == nil {
		logger = zap.NewNop()
	}
	if nodes == nil {
		nodes = NewAtomicNodeRegistry(nil, defaultObservedReportTTL)
	}
	s := &Service{
		logger:    logger,
		nodes:     nodes,
		strategy:  strategy,
		store:     store,
		artifacts: NewInMemoryArtifactStore(defaultArtifactStoreCapacity, 0),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// ServiceOption configures optional Service behaviour.
type ServiceOption func(*Service)

// WithNodeResourceLimit sets per-node resource thresholds for scheduling.
func WithNodeResourceLimit(limit *config.NodeResourceLimit) ServiceOption {
	return func(s *Service) {
		s.resourceLimit = limit
	}
}

func WithArtifactStore(store ArtifactStore) ServiceOption {
	return func(s *Service) {
		s.artifacts = store
	}
}

// WithNodeCapacityProvider sets the source of the Kubernetes allocatable
// memory used by the admission rule. Without it the rule keeps memory_total.
func WithNodeCapacityProvider(provider NodeCapacityProvider) ServiceOption {
	return func(s *Service) {
		s.capacityProvider = provider
	}
}

// WithAdmissionReservation enables the in-flight reservation ledger: every
// admitted create and resume reserves its shape on the selected node until the
// gateway confirms or rejects it, or the TTL elapses. A non-positive ttl
// leaves the ledger disabled, which restores the pre-reservation behavior.
func WithAdmissionReservation(ttl time.Duration) ServiceOption {
	return func(s *Service) {
		if ttl > 0 {
			s.reservations = newReservationLedger(ttl, time.Now)
		}
	}
}

type QueryOnlyService struct {
	schedulerv1.UnimplementedSchedulerServer
	logger *zap.Logger
	store  BindingStore
}

func NewQueryOnlyService(logger *zap.Logger, store BindingStore) *QueryOnlyService {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &QueryOnlyService{logger: logger, store: store}
}

func (s *QueryOnlyService) LookupNode(_ context.Context, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	return lookupNode(s.logger, s.store, req)
}

func (s *Service) Schedule(_ context.Context, req *schedulerv1.ScheduleRequest) (resp *schedulerv1.ScheduleResponse, err error) {
	start := time.Now()
	defer func() {
		recordSchedulerSchedule(s.strategy.Name(), start, err)
	}()

	// Reading the nodes, projecting what is already reserved, filtering and
	// selecting happen under one lock so two simultaneous creates cannot both
	// see the same free capacity. With the ledger disabled the lock is a no-op
	// and the path is exactly the historical one.
	s.lockAdmission()
	defer s.unlockAdmission()

	discovered := s.nodes.Snapshot( /* allowLingering */ false)
	rich := make([]RichNode, 0, len(discovered))
	for _, n := range discovered {
		snapshot, observedAt := s.nodes.PeekObservedAt(n.ID)
		rich = append(rich, RichNode{
			Node:       n,
			Snapshot:   snapshot,
			ObservedAt: observedAt,
		})
	}
	rich = s.annotateCapacity(rich)
	rich = s.projectReservations(rich)

	eligible := FilterByResourceLimitObservedBy(rich, s.resourceLimit, ResourceLimitObservers{
		Shadow: func(nodeID string, nominal, request bool, detail string) {
			s.logger.Info("admission shadow divergence",
				zap.String("node_id", nodeID),
				zap.Bool("nominal_admits", nominal),
				zap.Bool("request_admits", request),
				zap.String("detail", detail),
			)
		},
		Reject: func(nodeID string, violation LimitViolation) {
			fields := []zap.Field{
				zap.String("node_id", nodeID),
				zap.String("limit", violation.Limit),
				zap.String("observed", violation.Observed),
				zap.String("threshold", violation.Threshold),
				zap.String("meter", violation.Meter),
			}
			if violation.Detail != "" {
				fields = append(fields, zap.String("detail", violation.Detail))
			}
			s.logger.Info("scheduler rejected node by resource limit", fields...)
		},
		SkipDisk: func(nodeID string, violation LimitViolation) {
			s.logger.Info("scheduler skipped disk limit for node",
				zap.String("node_id", nodeID),
				zap.String("limit", violation.Limit),
				zap.String("threshold", violation.Threshold),
				zap.String("detail", violation.Detail),
			)
		},
		MeterFallback: func(nodeID string) {
			s.logger.Info("scheduler memory meter fell back to raw",
				zap.String("node_id", nodeID),
				zap.String("configured_meter", config.MemoryUsedMeterWorkingSet),
				zap.String("reason", "memory_working_set_bytes_absent"),
			)
		},
	})

	// Project the incoming create shape onto each candidate so a node only
	// admits the new sandbox when it fits the same capacity rule the resume
	// path uses. A create whose shape is unknown still reserves one slot.
	hint := req.GetHint()
	bodyMemory, bodyCPU, bodyHasShape, reserve := reservationShapeFromHint(hint)
	shape, shapeErr := templateShapeFromHint(hint)
	if shapeErr != nil {
		// An enum value this binary does not know is treated as a failed
		// lookup: never admitted blind when the shape rule decides.
		s.logger.Warn("scheduler ignored unknown template shape status",
			zap.String("template_id", shape.templateID),
			zap.Error(shapeErr),
		)
		shape.status = schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED
	}
	mode := s.resourceLimit.ResolvedShapeAdmission()

	// A create whose template shape could not be read must not be admitted
	// blind once the shape rule decides and a memory ceiling exists.
	if reserve && !bodyHasShape && mode == config.ShapeAdmissionOn &&
		memoryLimitConfigured(s.resourceLimit) && shape.lookupFailed() {
		recordShapeAdmission(shapeKindCreate, shapeOutcomeUnknownShape, shapeReasonUnknownTemplateShape)
		s.logger.Warn("scheduler refused create with unknown template shape",
			zap.String("kind", shapeKindCreate),
			zap.String("template_id", shape.templateID),
			zap.String("reason", shapeReasonUnknownTemplateShape),
		)
		return &schedulerv1.ScheduleResponse{
			RefusalReason: schedulerv1.ScheduleRefusalReason_SCHEDULE_REFUSAL_REASON_UNKNOWN_TEMPLATE_SHAPE,
		}, nil
	}

	reserveMemory, reserveCPU := bodyMemory, bodyCPU
	switch {
	case bodyHasShape:
		// The body shape filters exactly as before in every mode; shadow only
		// records the verdict on top of the same decision.
		eligible = s.projectCreateShape(eligible, bodyMemory, bodyCPU, shape, true)
	case reserve && mode == config.ShapeAdmissionOn && shape.resolved():
		reserveMemory, reserveCPU = shape.memoryBytes, shape.cpuCount
		eligible = s.projectCreateShape(eligible, shape.memoryBytes, shape.cpuCount, shape, true)
	case reserve && mode == config.ShapeAdmissionShadow:
		switch shape.status {
		case schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_RESOLVED:
			// Record the verdict per node without changing the decision.
			s.projectCreateShape(eligible, shape.memoryBytes, shape.cpuCount, shape, false)
		case schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_NOT_FOUND:
			// The template does not exist; the upstream create decides.
		default:
			s.recordUnknownCreateShape(shape)
		}
	}

	node, selectErr := s.strategy.Select(eligible, req.GetHint())
	if selectErr != nil {
		s.logger.Debug("scheduler selection failed",
			zap.String("strategy", s.strategy.Name()),
			zap.String("hint", summarizeScheduleHint(req.GetHint())),
			zap.Int("candidate_nodes", len(rich)),
			zap.Int("eligible_nodes", len(eligible)),
			zap.Error(selectErr),
		)
		if errors.Is(selectErr, ErrNoNodes) {
			err = status.Error(codes.Unavailable, "no nodes available")
			return nil, err
		}
		err = status.Error(codes.Internal, selectErr.Error())
		return nil, err
	}
	s.logger.Debug("scheduler selected node",
		zap.String("strategy", s.strategy.Name()),
		zap.String("hint", summarizeScheduleHint(req.GetHint())),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
		zap.Int("candidate_nodes", len(rich)),
		zap.Int("eligible_nodes", len(eligible)),
	)
	reservationID := ""
	if reserve {
		reservationID = s.reserve(node.ID, reserveMemory, reserveCPU, "")
	}
	return &schedulerv1.ScheduleResponse{
		Node:          node.Node.ToProto(),
		ReservationId: reservationID,
	}, nil
}

// reservationShapeFromHint resolves the shape a create reserves. reserve is
// false only when the hint does not describe an allocating request (for
// example a request that does not create a sandbox). hasShape is false when
// the hint is a create whose template shape the gateway could not read; the
// decision then reserves a slot only.
func reservationShapeFromHint(hint *schedulerv1.ScheduleRequestHint) (memoryBytes uint64, cpuCount uint32, hasShape bool, reserve bool) {
	switch kind := hint.GetKind().(type) {
	case *schedulerv1.ScheduleRequestHint_NewColdSandbox:
		cold := kind.NewColdSandbox
		if cold == nil || cold.GetMemoryMb() == 0 {
			return 0, 0, false, true
		}
		return cold.GetMemoryMb() << 20, cold.GetCpuCount(), true, true
	case *schedulerv1.ScheduleRequestHint_NewSandbox:
		sandbox := kind.NewSandbox
		if sandbox == nil || sandbox.GetMemoryMb() == 0 {
			return 0, 0, false, true
		}
		return sandbox.GetMemoryMb() << 20, sandbox.GetCpuCount(), true, true
	default:
		return 0, 0, false, false
	}
}

// templateShape is the template shape the gateway resolved before scheduling a
// create. status distinguishes a genuine shape from a missing or unreadable
// template so the scheduler never admits blind when the rule decides.
type templateShape struct {
	memoryBytes uint64
	cpuCount    uint32
	templateID  string
	status      schedulerv1.TemplateShapeStatus
}

// templateShapeFromHint interprets NewSandboxHint.template_shape_status with an
// exhaustive switch. An unknown enum value is a typed error the caller logs and
// treats as a failed lookup, never as an admission.
func templateShapeFromHint(hint *schedulerv1.ScheduleRequestHint) (templateShape, error) {
	sandbox := hint.GetNewSandbox()
	if sandbox == nil {
		return templateShape{status: schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_UNSPECIFIED}, nil
	}
	shape := templateShape{
		memoryBytes: sandbox.GetResolvedMemoryMb() << 20,
		cpuCount:    sandbox.GetResolvedCpuCount(),
		templateID:  sandbox.GetTemplateId(),
		status:      sandbox.GetTemplateShapeStatus(),
	}
	switch shape.status {
	case schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_UNSPECIFIED,
		schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_RESOLVED,
		schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_NOT_FOUND,
		schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED,
		schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_UNAVAILABLE:
		return shape, nil
	default:
		return shape, fmt.Errorf("unknown template_shape_status %d", int32(shape.status))
	}
}

// resolved reports whether the hint carries a usable template shape.
func (s templateShape) resolved() bool {
	return s.status == schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_RESOLVED && s.memoryBytes > 0
}

// lookupFailed reports whether the template read failed in a way that leaves
// the shape unknown.
func (s templateShape) lookupFailed() bool {
	switch s.status {
	case schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED,
		schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_UNAVAILABLE:
		return true
	default:
		return false
	}
}

// memoryLimitConfigured reports whether a memory allocation ceiling exists, so
// the shape rule has something to enforce.
func memoryLimitConfigured(limit *config.NodeResourceLimit) bool {
	return limit != nil && limit.MaxMemoryAllocatedPercent != nil
}

// projectCreateShape applies the shape rule to the candidates. enforce drops
// the nodes that do not admit the shape; with it false every candidate is
// returned unchanged, because shadow must not change the decision. When shadow
// is active it also records a verdict per node. The body shape is always
// enforced, in every mode, exactly as before the shape rule existed; the
// resolved template shape is enforced only in "on" and merely observed in
// "shadow".
func (s *Service) projectCreateShape(nodes []RichNode, memoryBytes uint64, cpuCount uint32, shape templateShape, enforce bool) []RichNode {
	shadow := s.resourceLimit != nil && s.resourceLimit.ShapeAdmissionShadow()
	admitted := make([]RichNode, 0, len(nodes))
	for _, node := range nodes {
		ok, reason := EvaluateShapeAdmission(node, memoryBytes, cpuCount, s.resourceLimit)
		if ok {
			admitted = append(admitted, node)
		}
		if shadow {
			outcome := shapeOutcomeWouldReject
			if ok {
				outcome = shapeOutcomeAdmit
			}
			s.recordCreateShapeVerdict(outcome, reason, shape, memoryBytes, node)
		}
	}
	if shadow && len(nodes) > 0 && len(admitted) == 0 {
		recordShapeAdmission(shapeKindCreate, shapeOutcomeWouldRejectAll, shapeReasonNone)
	}
	if !enforce {
		return nodes
	}
	return admitted
}

// recordCreateShapeVerdict counts and logs one create shape verdict.
func (s *Service) recordCreateShapeVerdict(outcome, reason string, shape templateShape, memoryBytes uint64, node RichNode) {
	recordShapeAdmission(shapeKindCreate, outcome, reason)
	s.logger.Info("scheduler shape admission verdict",
		zap.String("kind", shapeKindCreate),
		zap.String("node_id", node.Node.ID),
		zap.String("template_id", shape.templateID),
		zap.Uint64("shape_bytes", memoryBytes),
		zap.Uint64("allocated_bytes", node.Snapshot.GetAllocatedMemoryBytes()),
		zap.String("reason", reasonOrNone(reason)),
		zap.String("limit", limitMaxMemoryAllocatedPercent),
		zap.Uint32("threshold", thresholdPercent(s.resourceLimit)),
		zap.String("outcome", outcome),
	)
}

// recordUnknownCreateShape counts a create whose shape could not be resolved.
func (s *Service) recordUnknownCreateShape(shape templateShape) {
	recordShapeAdmission(shapeKindCreate, shapeOutcomeUnknownShape, shapeReasonUnknownShape)
	s.logger.Info("scheduler create has no known shape",
		zap.String("kind", shapeKindCreate),
		zap.String("template_id", shape.templateID),
		zap.String("reason", shapeReasonUnknownShape),
	)
}

func reasonOrNone(reason string) string {
	if reason == "" {
		return shapeReasonNone
	}
	return reason
}

func thresholdPercent(limit *config.NodeResourceLimit) uint32 {
	if limit == nil || limit.MaxMemoryAllocatedPercent == nil {
		return 0
	}
	return *limit.MaxMemoryAllocatedPercent
}

// observeResumeShadow evaluates the resume rule and records the verdict while
// leaving the decision untouched.
func (s *Service) observeResumeShadow(sandboxID string, owner Node, req *schedulerv1.ScheduleResumeRequest) {
	s.lockAdmission()
	defer s.unlockAdmission()

	memoryBytes, cpuCount, known := s.pausedShape(owner.ID, sandboxID, req)
	if !known {
		recordShapeAdmission(shapeKindResume, shapeOutcomeUnknownShape, shapeReasonUnknownShape)
		s.logger.Warn("scheduler shadow deferred resume with unknown paused shape",
			zap.String("kind", shapeKindResume),
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", owner.ID),
			zap.String("reason", shapeReasonUnknownShape),
		)
		return
	}

	ownerRich := s.projectReservationsExcept([]RichNode{s.richNode(owner.ID)}, sandboxID)[0]
	admitted, reason := EvaluateShapeAdmission(ownerRich, memoryBytes, cpuCount, s.resourceLimit)
	if admitted {
		recordShapeAdmission(shapeKindResume, shapeOutcomeAdmit, shapeReasonNone)
		s.logResumeShadow(shapeOutcomeAdmit, reason, sandboxID, owner.ID, memoryBytes, ownerRich)
		return
	}
	if s.resourceLimit.CrossNodeResumeEnabled() {
		if candidates := s.resumeCandidates(owner.ID, memoryBytes, cpuCount, sandboxID); len(candidates) > 0 {
			recordShapeAdmission(shapeKindResume, shapeOutcomeWouldMove, reason)
			s.logResumeShadow(shapeOutcomeWouldMove, reason, sandboxID, owner.ID, memoryBytes, ownerRich)
			return
		}
	}
	recordShapeAdmission(shapeKindResume, shapeOutcomeWouldDefer, reason)
	s.logResumeShadow(shapeOutcomeWouldDefer, reason, sandboxID, owner.ID, memoryBytes, ownerRich)
}

func (s *Service) logResumeShadow(outcome, reason, sandboxID, nodeID string, memoryBytes uint64, ownerRich RichNode) {
	s.logger.Info("scheduler shape admission verdict",
		zap.String("kind", shapeKindResume),
		zap.String("outcome", outcome),
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", nodeID),
		zap.Uint64("shape_bytes", memoryBytes),
		zap.Uint64("allocated_bytes", ownerRich.Snapshot.GetAllocatedMemoryBytes()),
		zap.String("reason", reasonOrNone(reason)),
		zap.String("limit", limitMaxMemoryAllocatedPercent),
		zap.Uint32("threshold", thresholdPercent(s.resourceLimit)),
	)
}

// lockAdmission serializes the decision with the in-flight reservations. It
// is a no-op when the ledger is disabled.
func (s *Service) lockAdmission() {
	if s.reservations != nil {
		s.admissionMu.Lock()
	}
}

func (s *Service) unlockAdmission() {
	if s.reservations != nil {
		s.admissionMu.Unlock()
	}
}

// reserve records an admitted shape on the node. key is the sandbox ID for a
// resume (so a repeated resume replaces its own entry) and empty for a create.
func (s *Service) reserve(nodeID string, memoryBytes uint64, cpuCount uint32, key string) string {
	if s.reservations == nil {
		return ""
	}
	return s.reservations.Reserve(nodeID, memoryBytes, cpuCount, 1, key)
}

// settleReservation releases one reservation. It is best-effort and
// idempotent: an unknown or repeated id changes nothing.
func (s *Service) settleReservation(id string, outcome string) bool {
	if s.reservations == nil || id == "" {
		return false
	}
	return s.reservations.Settle(id, outcome)
}

// projectReservations returns a copy of the candidates with every in-flight
// reservation added to the fields the admission rule reads. The stored
// heartbeat snapshot is never mutated; the copy is what the filters see.
func (s *Service) projectReservations(nodes []RichNode) []RichNode {
	return s.projectReservationsExcept(nodes, "")
}

// projectReservationsExcept is projectReservations without the reservation
// carrying excludeKey, used when the decision is a retry of that same
// reservation (a repeated resume of one sandbox).
func (s *Service) projectReservationsExcept(nodes []RichNode, excludeKey string) []RichNode {
	if s.reservations == nil || len(nodes) == 0 {
		return nodes
	}
	projected := make([]RichNode, len(nodes))
	for i, node := range nodes {
		// A confirmed reservation stops projecting as soon as the node reports
		// a snapshot produced after the confirmation: at that point the
		// snapshot itself counts the admitted sandbox and holding the
		// reservation too would double count it.
		s.reservations.Observe(node.ID, node.ObservedAt)
		memoryBytes, cpuCount, slots, count := s.reservations.ProjectExcept(node.ID, excludeKey)
		if count == 0 || node.Snapshot == nil {
			projected[i] = node
			continue
		}
		snapshot := cloneSnapshot(node.Snapshot)
		snapshot.AllocatedMemoryBytes += memoryBytes
		// The reserved bytes are committed to this node even though the guest
		// has not started using them. Adding them to memory_used as well keeps
		// memory_used - allocated (the runtime overhead the capacity base
		// subtracts) unchanged, so the projection cannot inflate the ceiling.
		snapshot.MemoryUsedBytes += memoryBytes
		// The working set is projected the same way, but only when the node
		// reports it: an absent field must stay absent so the meter falls
		// back to the raw figure instead of reading the reservation alone.
		if snapshot.MemoryWorkingSetBytes != nil {
			snapshot.MemoryWorkingSetBytes = proto.Uint64(snapshot.GetMemoryWorkingSetBytes() + memoryBytes)
		}
		if snapshot.GetRequestedMemoryBytes() > 0 {
			snapshot.RequestedMemoryBytes += memoryBytes
		}
		snapshot.AllocatedCpu += cpuCount
		snapshot.SandboxCount += slots
		snapshot.SandboxStartingCount += slots
		projected[i] = RichNode{Node: node.Node, Snapshot: snapshot, ObservedAt: node.ObservedAt, Capacity: node.Capacity}
	}
	return projected
}

// summarizeScheduleHint renders a compact, log-friendly description of a
// scheduling hint.
func summarizeScheduleHint(hint *schedulerv1.ScheduleRequestHint) string {
	switch k := hint.GetKind().(type) {
	case *schedulerv1.ScheduleRequestHint_NewColdSandbox:
		c := k.NewColdSandbox
		return fmt.Sprintf("new_cold_sandbox cpu=%d memory_mb=%d images=%v", c.GetCpuCount(), c.GetMemoryMb(), c.GetImages())
	case *schedulerv1.ScheduleRequestHint_NewSandbox:
		return "new_sandbox"
	default:
		return "none"
	}
}

func (s *Service) ListNodes(_ context.Context, _ *schedulerv1.ListNodesRequest) (*schedulerv1.ListNodesResponse, error) {
	snapshot := s.nodes.Snapshot( /* allowLingering */ true)
	nodes := make([]*schedulerv1.Node, 0, len(snapshot))
	for _, node := range snapshot {
		nodes = append(nodes, node.ToProto())
	}

	s.logger.Debug("scheduler listed nodes", zap.Int("node_count", len(nodes)))

	return &schedulerv1.ListNodesResponse{Nodes: nodes}, nil
}

func (s *Service) LookupNode(_ context.Context, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	return lookupNode(s.logger, s.store, req)
}

func lookupNode(logger *zap.Logger, store BindingStore, req *schedulerv1.LookupNodeRequest) (*schedulerv1.LookupNodeResponse, error) {
	if strings.TrimSpace(req.GetSandboxId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	node, ok, getErr := store.Get(req.GetSandboxId(), time.Now())
	if getErr != nil {
		logger.Warn("scheduler lookup binding store failed", zap.String("sandbox_id", req.GetSandboxId()), zap.Error(getErr))
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	if !ok {
		logger.Debug("scheduler lookup missed sandbox assignment", zap.String("sandbox_id", req.GetSandboxId()))
		return nil, status.Error(codes.NotFound, "sandbox assignment not found")
	}
	logger.Debug("scheduler lookup resolved sandbox assignment",
		zap.String("sandbox_id", req.GetSandboxId()),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
	)
	return &schedulerv1.LookupNodeResponse{Node: node.ToProto()}, nil
}

func (s *Service) RecordAssignment(_ context.Context, req *schedulerv1.RecordAssignmentRequest) (*schedulerv1.RecordAssignmentResponse, error) {
	if strings.TrimSpace(req.GetSandboxId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	node := NodeFromProto(req.GetNode())
	if strings.TrimSpace(node.ID) == "" || strings.TrimSpace(node.Endpoint) == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and endpoint are required")
	}
	if !s.isKnownNode(node) {
		s.logger.Warn("scheduler rejected assignment for unknown node",
			zap.String("sandbox_id", req.GetSandboxId()),
			zap.String("node_id", node.ID),
			zap.String("endpoint", node.Endpoint),
		)
		return nil, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
	}
	if err := s.store.Record(req.GetSandboxId(), node, time.Now()); err != nil {
		s.logger.Warn("scheduler record assignment binding store failed",
			zap.String("sandbox_id", req.GetSandboxId()),
			zap.String("node_id", node.ID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	s.logger.Debug("scheduler recorded sandbox assignment",
		zap.String("sandbox_id", req.GetSandboxId()),
		zap.String("node_id", node.ID),
		zap.String("endpoint", node.Endpoint),
	)
	return &schedulerv1.RecordAssignmentResponse{}, nil
}

func (s *Service) Heartbeat(_ context.Context, req *schedulerv1.HeartbeatRequest) (*schedulerv1.HeartbeatResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	serviceInstanceID := strings.TrimSpace(req.GetServiceInstanceId())
	if nodeID == "" || serviceInstanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and service_instance_id are required")
	}

	now := time.Now()
	node, cpuConfigJSON, err := s.nodes.Heartbeat(req, now)
	if err != nil {
		if errors.Is(err, ErrNodeNotInRegistry) {
			s.logger.Warn("scheduler rejected observed registration for unknown node",
				zap.String("node_id", nodeID),
			)
			return nil, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
		}
		return nil, status.Error(codes.Internal, "node registry heartbeat failed")
	}
	if err := s.store.ReconcileNode(node, req.GetSandboxIds(), now); err != nil {
		s.logger.Warn("scheduler heartbeat binding reconcile failed",
			zap.String("node_id", nodeID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	return &schedulerv1.HeartbeatResponse{CpuConfigJson: cpuConfigJSON}, nil
}

func (s *Service) ReportSandboxEvent(_ context.Context, req *schedulerv1.ReportSandboxEventRequest) (*schedulerv1.ReportSandboxEventResponse, error) {
	s.logger.Debug("scheduler ignored sandbox event batch",
		zap.String("node_id", req.GetNodeId()),
		zap.String("service_instance_id", req.GetServiceInstanceId()),
		zap.Int("event_count", len(req.GetEvents())),
	)
	return &schedulerv1.ReportSandboxEventResponse{}, nil
}

func (s *Service) RunObservedNodesMetrics(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	s.refreshObservedNodesMetrics(time.Now())

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.refreshObservedNodesMetrics(now)
		}
	}
}

func (s *Service) refreshObservedNodesMetrics(now time.Time) {
	recordObservedNodes(s.nodes.ListObserved("", now))
}

func (s *Service) ListObservedNodes(_ context.Context, req *schedulerv1.ListObservedNodesRequest) (*schedulerv1.ListObservedNodesResponse, error) {
	nodes := s.nodes.ListObserved(req.GetClusterId(), time.Now())
	rich := make([]RichNode, 0, len(nodes))
	for _, node := range nodes {
		rich = append(rich, RichNode{
			Node:       Node{ID: node.GetNodeId(), Endpoint: node.GetEndpoint()},
			Snapshot:   node.GetSnapshot(),
			ObservedAt: observedAtOf(node.GetSnapshot(), node.GetLastSeenUnixMs()),
		})
	}
	rich = s.annotateCapacity(rich)
	// The published verdict must reflect what is already in flight, or /nodes
	// would report a layer as admitted while an admitted create has reserved
	// the room it needs.
	rich = s.projectReservations(rich)
	for i, node := range nodes {
		node.Admission = s.nodeAdmission(rich[i])
	}
	return &schedulerv1.ListObservedNodesResponse{
		Nodes: nodes,
	}, nil
}

// nodeAdmission publishes the per-layer verdict for one observed node, along
// with the capacity facts it was computed over.
func (s *Service) nodeAdmission(n RichNode) *schedulerv1.NodeAdmission {
	if n.Snapshot == nil || s.resourceLimit == nil || !s.resourceLimit.ShapeAdmissionObserved() {
		return nil
	}
	verdicts := EvaluateNodeAdmission(n, s.resourceLimit)
	capacity, source := capacityMemoryBytes(n.Snapshot, n.Capacity, s.resourceLimit)
	admission := &schedulerv1.NodeAdmission{
		RuntimeOverheadBytes: runtimeOverheadBytes(n.Snapshot),
		CapacityMemoryBytes:  capacity,
		CapacitySource:       source,
	}
	if n.Capacity != nil {
		admission.AllocatableMemoryBytes = n.Capacity.AllocatableMemoryBytes
	}
	if s.reservations != nil {
		admission.ReservedMemoryBytes, admission.ReservedCpu, _, _ = s.reservations.Project(n.ID)
	}
	for _, verdict := range verdicts {
		admission.Layers = append(admission.Layers, &schedulerv1.AdmissionVerdict{
			MemoryGb: verdict.MemoryGB,
			Admitted: verdict.Admitted,
			Reason:   verdict.Reason,
		})
		reason := verdict.Reason
		if reason == "" {
			reason = "none"
		}
		schedulerNodeAdmission.WithLabelValues(
			strconv.FormatUint(verdict.MemoryGB, 10),
			strconv.FormatBool(verdict.Admitted),
			reason,
		).Inc()
	}
	return admission
}

// ScheduleResume gates a paused sandbox resume with the same capacity rule a
// new sandbox goes through. The owner is returned when the shape fits; when it
// does not, the sandbox is deferred (or moved to an admitted node when
// cross_node_resume is on). The binding changes only after a node is selected.
func (s *Service) ScheduleResume(_ context.Context, req *schedulerv1.ScheduleResumeRequest) (*schedulerv1.ScheduleResumeResponse, error) {
	sandboxID := strings.TrimSpace(req.GetSandboxId())
	if sandboxID == "" {
		return nil, status.Error(codes.InvalidArgument, "sandbox_id is required")
	}
	now := time.Now()
	owner, ok, getErr := s.store.Get(sandboxID, now)
	if getErr != nil {
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	if !ok {
		return nil, status.Error(codes.NotFound, "sandbox assignment not found")
	}

	if s.resourceLimit == nil || !s.resourceLimit.ShapeAdmissionObserved() {
		recordResumeAdmission("disabled")
		return &schedulerv1.ScheduleResumeResponse{Node: owner.ToProto(), Admitted: true}, nil
	}

	if s.resourceLimit.ShapeAdmissionShadow() {
		// Shadow evaluates the rule and records the verdict but keeps today's
		// decision: the owner is admitted with no reservation and no binding
		// change.
		s.observeResumeShadow(sandboxID, owner, req)
		recordResumeAdmission("shadow")
		return &schedulerv1.ScheduleResumeResponse{Node: owner.ToProto(), Admitted: true}, nil
	}

	plan := s.planResume(sandboxID, owner, req)
	switch {
	case !plan.known:
		// No shape means the projection cannot be evaluated. Defer instead of
		// over-admitting: fail-closed on an unknown shape.
		recordResumeAdmission("unknown_shape")
		s.logger.Warn("scheduler deferred resume with unknown paused shape",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", owner.ID),
		)
		return &schedulerv1.ScheduleResumeResponse{Node: owner.ToProto(), Admitted: false}, nil
	case plan.admitted && !plan.moved:
		recordResumeAdmission("owner")
		return &schedulerv1.ScheduleResumeResponse{
			Node:          plan.node.ToProto(),
			Admitted:      true,
			ReservationId: plan.reservationID,
		}, nil
	case plan.admitted:
		// The binding moves only after a node was selected; the reservation was
		// already made on that node under the decision lock, so a failed write
		// releases it instead of leaving a phantom.
		if recordErr := s.store.Record(sandboxID, plan.node, time.Now()); recordErr != nil {
			s.settleReservation(plan.reservationID, ReservationFailed)
			recordResumeAdmission("error")
			return nil, status.Error(codes.Unavailable, "binding store unavailable")
		}
		recordResumeAdmission("moved")
		s.logger.Info("scheduler moved a resume to another node",
			zap.String("sandbox_id", sandboxID),
			zap.String("from_node_id", owner.ID),
			zap.String("to_node_id", plan.node.ID),
			zap.String("reason", plan.reason),
		)
		return &schedulerv1.ScheduleResumeResponse{
			Node:          plan.node.ToProto(),
			Admitted:      true,
			ReservationId: plan.reservationID,
		}, nil
	default:
		recordResumeAdmission("deferred")
		s.logger.Info("scheduler deferred resume that does not fit the owner",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", owner.ID),
			zap.String("reason", plan.reason),
		)
		return &schedulerv1.ScheduleResumeResponse{Node: owner.ToProto(), Admitted: false, Reason: plan.reason}, nil
	}
}

// resumePlan is the outcome of one resume decision. The reservation, when
// admitted, is already recorded on the selected node; the caller only writes
// the binding after the lock is released.
type resumePlan struct {
	known         bool
	admitted      bool
	moved         bool
	reason        string
	node          Node
	reservationID string
}

// planResume decides where a paused sandbox resumes and reserves its shape
// there. The whole read-project-decide-reserve sequence runs under the
// admission lock, and the reservation of a repeated resume replaces the
// sandbox's own previous reservation instead of counting it twice.
func (s *Service) planResume(sandboxID string, owner Node, req *schedulerv1.ScheduleResumeRequest) resumePlan {
	s.lockAdmission()
	defer s.unlockAdmission()

	memoryBytes, cpuCount, known := s.pausedShape(owner.ID, sandboxID, req)
	if !known {
		return resumePlan{known: false}
	}

	ownerRich := s.projectReservationsExcept([]RichNode{s.richNode(owner.ID)}, sandboxID)[0]
	admitted, reason := EvaluateShapeAdmission(ownerRich, memoryBytes, cpuCount, s.resourceLimit)
	if admitted {
		return resumePlan{
			known:         true,
			admitted:      true,
			node:          owner,
			reservationID: s.reserve(owner.ID, memoryBytes, cpuCount, sandboxID),
		}
	}
	if !s.resourceLimit.CrossNodeResumeEnabled() {
		return resumePlan{known: true, reason: reason, node: owner}
	}

	candidates := s.resumeCandidates(owner.ID, memoryBytes, cpuCount, sandboxID)
	selected, selectErr := s.strategy.Select(candidates, nil)
	if selectErr != nil {
		return resumePlan{known: true, reason: reason, node: owner}
	}
	return resumePlan{
		known:         true,
		admitted:      true,
		moved:         true,
		node:          selected.Node,
		reservationID: s.reserve(selected.ID, memoryBytes, cpuCount, sandboxID),
	}
}

// SettleAdmission releases a reservation once the gateway knows whether the
// create or resume succeeded. Settlement is best-effort and idempotent: an
// unknown or repeated reservation is a no-op, never an error.
func (s *Service) SettleAdmission(_ context.Context, req *schedulerv1.SettleAdmissionRequest) (*schedulerv1.SettleAdmissionResponse, error) {
	reservationID := strings.TrimSpace(req.GetReservationId())
	if reservationID == "" {
		return nil, status.Error(codes.InvalidArgument, "reservation_id is required")
	}
	outcome := strings.TrimSpace(req.GetOutcome())
	if outcome != ReservationConfirmed && outcome != ReservationFailed {
		return nil, status.Error(codes.InvalidArgument, "outcome must be confirmed or failed")
	}
	settled := s.settleReservation(reservationID, outcome)
	s.logger.Debug("scheduler settled admission reservation",
		zap.String("reservation_id", reservationID),
		zap.String("outcome", outcome),
		zap.Bool("settled", settled),
	)
	return &schedulerv1.SettleAdmissionResponse{}, nil
}

// pausedShape resolves the persisted shape of a paused sandbox: the owner's
// heartbeat report first, then the gateway-supplied template shape.
func (s *Service) pausedShape(nodeID, sandboxID string, req *schedulerv1.ScheduleResumeRequest) (uint64, uint32, bool) {
	if cpuCount, memoryBytes, ok := s.nodes.PausedSandboxResources(nodeID, sandboxID); ok && memoryBytes > 0 {
		return memoryBytes, cpuCount, true
	}
	if req.GetMemoryBytes() > 0 {
		return req.GetMemoryBytes(), req.GetCpuCount(), true
	}
	return 0, 0, false
}

// richNode returns the rich view of one discovered node, with its capacity.
func (s *Service) richNode(nodeID string) RichNode {
	node, _ := s.nodes.Resolve(nodeID)
	snapshot, observedAt := s.nodes.PeekObservedAt(nodeID)
	rich := []RichNode{{Node: node, Snapshot: snapshot, ObservedAt: observedAt}}
	return s.annotateCapacity(rich)[0]
}

// resumeCandidates returns every node except the owner that admits the paused
// shape, using the existing scheduling filters. In-flight reservations, except
// the resuming sandbox's own (excludeKey), are projected before filtering.
func (s *Service) resumeCandidates(ownerID string, memoryBytes uint64, cpuCount uint32, excludeKey string) []RichNode {
	discovered := s.nodes.Snapshot( /* allowLingering */ false)
	rich := make([]RichNode, 0, len(discovered))
	for _, node := range discovered {
		if node.ID == ownerID {
			continue
		}
		snapshot, observedAt := s.nodes.PeekObservedAt(node.ID)
		rich = append(rich, RichNode{Node: node, Snapshot: snapshot, ObservedAt: observedAt})
	}
	rich = s.annotateCapacity(rich)
	rich = s.projectReservationsExcept(rich, excludeKey)
	rich = FilterByResourceLimit(rich, s.resourceLimit)
	return filterByShape(rich, memoryBytes, cpuCount, s.resourceLimit)
}

// filterByShape drops nodes whose projected snapshot with the given shape
// violates a capacity limit.
func filterByShape(nodes []RichNode, memoryBytes uint64, cpuCount uint32, limit *config.NodeResourceLimit) []RichNode {
	if limit == nil {
		return nodes
	}
	admitted := make([]RichNode, 0, len(nodes))
	for _, node := range nodes {
		if ok, _ := EvaluateShapeAdmission(node, memoryBytes, cpuCount, limit); ok {
			admitted = append(admitted, node)
		}
	}
	return admitted
}

func recordResumeAdmission(outcome string) {
	schedulerResumeAdmission.WithLabelValues(outcome).Inc()
}

func (s *Service) ListP2PPeers(_ context.Context, req *schedulerv1.ListP2PPeersRequest) (*schedulerv1.ListP2PPeersResponse, error) {
	peers := s.nodes.ListP2pPeers(
		req.GetClusterId(),
		req.GetBackend(),
		req.GetExcludeNodeId(),
		time.Now(),
	)
	return &schedulerv1.ListP2PPeersResponse{Peers: peers}, nil
}

func (s *Service) RecordP2PArtifact(_ context.Context, req *schedulerv1.RecordP2PArtifactRequest) (*schedulerv1.RecordP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" || strings.TrimSpace(req.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, key, and node_id are required")
	}
	if _, ok := s.nodes.Resolve(req.GetNodeId()); !ok {
		return nil, status.Error(codes.InvalidArgument, "node is not in scheduler node list")
	}

	s.artifacts.Record(req.GetClusterId(), req.GetBackend(), req.GetKey(), req.GetNodeId())
	s.logger.Debug("scheduler recorded P2P artifact",
		zap.String("cluster_id", req.GetClusterId()),
		zap.String("backend", req.GetBackend()),
		zap.String("key", req.GetKey()),
		zap.String("node_id", req.GetNodeId()),
	)
	return &schedulerv1.RecordP2PArtifactResponse{}, nil
}

func (s *Service) ForgetP2PArtifact(_ context.Context, req *schedulerv1.ForgetP2PArtifactRequest) (*schedulerv1.ForgetP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" || strings.TrimSpace(req.GetNodeId()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, key, and node_id are required")
	}

	s.artifacts.Forget(req.GetClusterId(), req.GetBackend(), req.GetKey(), req.GetNodeId())
	s.logger.Debug("scheduler forgot P2P artifact",
		zap.String("cluster_id", req.GetClusterId()),
		zap.String("backend", req.GetBackend()),
		zap.String("key", req.GetKey()),
		zap.String("node_id", req.GetNodeId()),
	)
	return &schedulerv1.ForgetP2PArtifactResponse{}, nil
}

func (s *Service) LookupP2PArtifact(_ context.Context, req *schedulerv1.LookupP2PArtifactRequest) (*schedulerv1.LookupP2PArtifactResponse, error) {
	if strings.TrimSpace(req.GetClusterId()) == "" || strings.TrimSpace(req.GetBackend()) == "" || strings.TrimSpace(req.GetKey()) == "" {
		return nil, status.Error(codes.InvalidArgument, "cluster_id, backend, and key are required")
	}

	nodeIDs := s.artifacts.Lookup(req.GetClusterId(), req.GetBackend(), req.GetKey())
	peers := s.nodes.FilterP2pPeers(
		req.GetClusterId(),
		req.GetBackend(),
		nodeIDs,
		req.GetExcludeNodeId(),
		time.Now(),
	)
	return &schedulerv1.LookupP2PArtifactResponse{Peers: peers}, nil
}

func (s *Service) GetNode(_ context.Context, req *schedulerv1.GetNodeRequest) (*schedulerv1.GetNodeResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	if nodeID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id is required")
	}

	node, ok := s.nodes.GetObserved(nodeID, req.GetClusterId(), time.Now())
	if !ok {
		return nil, status.Error(codes.NotFound, "observed node not found")
	}

	return &schedulerv1.GetNodeResponse{Node: node}, nil
}

func (s *Service) UnregisterNode(_ context.Context, req *schedulerv1.UnregisterNodeRequest) (*schedulerv1.UnregisterNodeResponse, error) {
	nodeID := strings.TrimSpace(req.GetNodeId())
	serviceInstanceID := strings.TrimSpace(req.GetServiceInstanceId())
	if nodeID == "" || serviceInstanceID == "" {
		return nil, status.Error(codes.InvalidArgument, "node_id and service_instance_id are required")
	}

	unregisterErr := s.nodes.UnregisterObserved(nodeID, serviceInstanceID)
	if unregisterErr != nil {
		if errors.Is(unregisterErr, ErrServiceInstanceMismatch) {
			return nil, status.Error(codes.FailedPrecondition, "service instance mismatch")
		}
		return nil, status.Error(codes.Internal, unregisterErr.Error())
	}

	now := time.Now()
	if err := s.store.ReconcileNode(Node{ID: nodeID}, nil, now); err != nil {
		s.logger.Warn("scheduler unregister binding reconcile failed",
			zap.String("node_id", nodeID),
			zap.Error(err),
		)
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	s.artifacts.ForgetNode(nodeID)

	return &schedulerv1.UnregisterNodeResponse{}, nil
}

func (s *Service) isKnownNode(node Node) bool {
	return s.nodes.Contains(node)
}
