package scheduler

import (
	"sync"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
)

// NodeCapacityProvider exposes the Kubernetes allocatable memory for an
// observed node (named by its pod). Static and local discovery do not have a
// provider, so the scheduler keeps the memory_total fallback.
type NodeCapacityProvider interface {
	Capacity(nodeID string) (NodeCapacity, bool)
}

// NodeCapacitySource is a late-bound provider. The scheduler service is built
// before Kubernetes discovery retries its in-cluster client, so it reads the
// provider through this holder.
type NodeCapacitySource struct {
	mu       sync.RWMutex
	provider NodeCapacityProvider
}

func NewNodeCapacitySource() *NodeCapacitySource {
	return &NodeCapacitySource{}
}

func (s *NodeCapacitySource) Set(provider NodeCapacityProvider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.provider = provider
}

func (s *NodeCapacitySource) Capacity(nodeID string) (NodeCapacity, bool) {
	s.mu.RLock()
	provider := s.provider
	s.mu.RUnlock()
	if provider == nil {
		return NodeCapacity{}, false
	}
	return provider.Capacity(nodeID)
}

// nodeCapacityIndex maps an agentenv pod name to its Kubernetes node name and
// keeps that node's status.allocatable.memory.
type nodeCapacityIndex struct {
	mu          sync.RWMutex
	podNodeName map[string]string
	allocatable map[string]uint64
}

func newNodeCapacityIndex() *nodeCapacityIndex {
	return &nodeCapacityIndex{
		podNodeName: make(map[string]string),
		allocatable: make(map[string]uint64),
	}
}

func (i *nodeCapacityIndex) replace(podNodeName map[string]string, allocatable map[string]uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.podNodeName = podNodeName
	i.allocatable = allocatable
}

// Capacity returns the allocatable memory of the Kubernetes node that hosts
// the pod. Both the pod mapping and the node figure must be known.
func (i *nodeCapacityIndex) Capacity(nodeID string) (NodeCapacity, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	nodeName, ok := i.podNodeName[nodeID]
	if !ok {
		return NodeCapacity{}, false
	}
	bytes, ok := i.allocatable[nodeName]
	if !ok || bytes == 0 {
		return NodeCapacity{}, false
	}
	return NodeCapacity{AllocatableMemoryBytes: bytes, Source: "kubernetes"}, true
}

// podNodeNames maps each endpoint's pod name to the Kubernetes node that runs
// it. EndpointSlice endpoints carry nodeName even when the pod itself is not
// readable from the scheduler's namespace.
func podNodeNames(slices []*discoveryv1.EndpointSlice) map[string]string {
	names := make(map[string]string)
	for _, slice := range slices {
		if slice == nil {
			continue
		}
		for _, endpoint := range slice.Endpoints {
			if endpoint.TargetRef == nil || endpoint.TargetRef.Name == "" {
				continue
			}
			if endpoint.NodeName == nil || *endpoint.NodeName == "" {
				continue
			}
			names[endpoint.TargetRef.Name] = *endpoint.NodeName
		}
	}
	return names
}

// allocatableMemoryFromNodes reads status.allocatable.memory for each
// Kubernetes node.
func allocatableMemoryFromNodes(nodes []*corev1.Node) map[string]uint64 {
	allocatable := make(map[string]uint64, len(nodes))
	for _, node := range nodes {
		if node == nil || node.Name == "" {
			continue
		}
		if bytes := node.Status.Allocatable.Memory().Value(); bytes > 0 {
			allocatable[node.Name] = uint64(bytes)
		}
	}
	return allocatable
}

// annotateCapacity sets RichNode.Capacity from the provider and counts the
// resolved source. It returns the nodes for convenience.
func (s *Service) annotateCapacity(nodes []RichNode) []RichNode {
	if s.capacityProvider == nil {
		for range nodes {
			schedulerCapacitySource.WithLabelValues("total").Inc()
		}
		return nodes
	}
	for i := range nodes {
		if capacity, ok := s.capacityProvider.Capacity(nodes[i].ID); ok {
			nodes[i].Capacity = &capacity
			schedulerCapacitySource.WithLabelValues(capacitySourceFor(nodes[i].Snapshot, &capacity, s.resourceLimit)).Inc()
			continue
		}
		schedulerCapacitySource.WithLabelValues("total").Inc()
	}
	return nodes
}

// capacitySourceFor resolves the source label for one node for the metric.
func capacitySourceFor(snapshot *schedulerv1.NodeSnapshot, capacity *NodeCapacity, limit *config.NodeResourceLimit) string {
	if snapshot == nil {
		return config.MemoryCapacitySourceTotal
	}
	_, source := capacityMemoryBytes(snapshot, capacity, limit)
	return source
}
