package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

	discovered := s.nodes.Snapshot( /* allowLingering */ false)
	rich := make([]RichNode, 0, len(discovered))
	for _, n := range discovered {
		rich = append(rich, RichNode{
			Node:     n,
			Snapshot: s.nodes.PeekObserved(n.ID),
		})
	}
	rich = s.annotateCapacity(rich)

	eligible := FilterByResourceLimitReported(rich, s.resourceLimit, func(nodeID string, nominal, request bool, detail string) {
		s.logger.Info("admission shadow divergence",
			zap.String("node_id", nodeID),
			zap.Bool("nominal_admits", nominal),
			zap.Bool("request_admits", request),
			zap.String("detail", detail),
		)
	}, func(nodeID string, violation LimitViolation) {
		fields := []zap.Field{
			zap.String("node_id", nodeID),
			zap.String("limit", violation.Limit),
			zap.String("observed", violation.Observed),
			zap.String("threshold", violation.Threshold),
		}
		if violation.Detail != "" {
			fields = append(fields, zap.String("detail", violation.Detail))
		}
		s.logger.Info("scheduler rejected node by resource limit", fields...)
	}, func(nodeID string, violation LimitViolation) {
		s.logger.Info("scheduler skipped disk limit for node",
			zap.String("node_id", nodeID),
			zap.String("limit", violation.Limit),
			zap.String("threshold", violation.Threshold),
			zap.String("detail", violation.Detail),
		)
	})

	// Project the cold-sandbox shape onto each candidate so a node only
	// admits the new sandbox when it fits the same capacity rule the resume
	// path uses.
	if cold := req.GetHint().GetNewColdSandbox(); cold != nil && cold.GetMemoryMb() > 0 {
		eligible = filterByShape(eligible, cold.GetMemoryMb()<<20, cold.GetCpuCount(), s.resourceLimit)
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
	return &schedulerv1.ScheduleResponse{Node: node.Node.ToProto()}, nil
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
			Node:     Node{ID: node.GetNodeId(), Endpoint: node.GetEndpoint()},
			Snapshot: node.GetSnapshot(),
		})
	}
	rich = s.annotateCapacity(rich)
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
	if n.Snapshot == nil || s.resourceLimit == nil || !s.resourceLimit.ShapeAdmissionEnabled() {
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

	if s.resourceLimit == nil || !s.resourceLimit.ShapeAdmissionEnabled() {
		recordResumeAdmission("disabled")
		return &schedulerv1.ScheduleResumeResponse{Node: owner.ToProto(), Admitted: true}, nil
	}

	memoryBytes, cpuCount, known := s.pausedShape(owner.ID, sandboxID, req)
	if !known {
		// No shape means the projection cannot be evaluated. Defer instead of
		// over-admitting: fail-closed on an unknown shape.
		recordResumeAdmission("unknown_shape")
		s.logger.Warn("scheduler deferred resume with unknown paused shape",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", owner.ID),
		)
		return &schedulerv1.ScheduleResumeResponse{Node: owner.ToProto(), Admitted: false}, nil
	}

	ownerRich := s.richNode(owner.ID)
	admitted, reason := EvaluateShapeAdmission(ownerRich, memoryBytes, cpuCount, s.resourceLimit)
	if admitted {
		recordResumeAdmission("owner")
		return &schedulerv1.ScheduleResumeResponse{Node: owner.ToProto(), Admitted: true}, nil
	}
	if !s.resourceLimit.CrossNodeResumeEnabled() {
		recordResumeAdmission("deferred")
		s.logger.Info("scheduler deferred resume that does not fit the owner",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", owner.ID),
			zap.String("reason", reason),
		)
		return &schedulerv1.ScheduleResumeResponse{Node: owner.ToProto(), Admitted: false, Reason: reason}, nil
	}

	candidates := s.resumeCandidates(owner.ID, memoryBytes, cpuCount)
	selected, selectErr := s.strategy.Select(candidates, nil)
	if selectErr != nil {
		recordResumeAdmission("deferred")
		s.logger.Info("scheduler deferred resume without an alternative node",
			zap.String("sandbox_id", sandboxID),
			zap.String("node_id", owner.ID),
			zap.String("reason", reason),
		)
		return &schedulerv1.ScheduleResumeResponse{Node: owner.ToProto(), Admitted: false, Reason: reason}, nil
	}
	if recordErr := s.store.Record(sandboxID, selected.Node, time.Now()); recordErr != nil {
		recordResumeAdmission("error")
		return nil, status.Error(codes.Unavailable, "binding store unavailable")
	}
	recordResumeAdmission("moved")
	s.logger.Info("scheduler moved a resume to another node",
		zap.String("sandbox_id", sandboxID),
		zap.String("from_node_id", owner.ID),
		zap.String("to_node_id", selected.ID),
		zap.String("reason", reason),
	)
	return &schedulerv1.ScheduleResumeResponse{Node: selected.Node.ToProto(), Admitted: true}, nil
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
	rich := []RichNode{{Node: node, Snapshot: s.nodes.PeekObserved(nodeID)}}
	return s.annotateCapacity(rich)[0]
}

// resumeCandidates returns every node except the owner that admits the paused
// shape, using the existing scheduling filters.
func (s *Service) resumeCandidates(ownerID string, memoryBytes uint64, cpuCount uint32) []RichNode {
	discovered := s.nodes.Snapshot( /* allowLingering */ false)
	rich := make([]RichNode, 0, len(discovered))
	for _, node := range discovered {
		if node.ID == ownerID {
			continue
		}
		rich = append(rich, RichNode{Node: node, Snapshot: s.nodes.PeekObserved(node.ID)})
	}
	rich = s.annotateCapacity(rich)
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
