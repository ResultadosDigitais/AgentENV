package scheduler

import schedulerv1 "agentenv/services/api/proto"

type Node struct {
	ID       string `json:"node_id"`
	Endpoint string `json:"endpoint"`
}

// RichNode combines discovery identity with observed runtime state.
// Strategy implementations can use Snapshot for load-aware scheduling.
type RichNode struct {
	Node
	Snapshot *schedulerv1.NodeSnapshot // nil if no heartbeat received yet
	// Capacity carries the node's Kubernetes allocatable memory when a
	// cluster-scoped node capacity provider is wired. Nil keeps the legacy
	// memory_total math.
	Capacity *NodeCapacity
}

// NodeCapacity is the physical memory fact the admission rule reads. The
// runtime overhead is not part of it: it is derived from the node's own
// snapshot (memory_used - allocated) so a stale allocatable cannot erase the
// runtime's own footprint.
type NodeCapacity struct {
	AllocatableMemoryBytes uint64
	// Source names where the allocatable figure came from ("kubernetes").
	Source string
}

func (n Node) ToProto() *schedulerv1.Node {
	return &schedulerv1.Node{
		NodeId:   n.ID,
		Endpoint: n.Endpoint,
	}
}

func NodeFromProto(node *schedulerv1.Node) Node {
	if node == nil {
		return Node{}
	}
	return Node{
		ID:       node.GetNodeId(),
		Endpoint: node.GetEndpoint(),
	}
}
