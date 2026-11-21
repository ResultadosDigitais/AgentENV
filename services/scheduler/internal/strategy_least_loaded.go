package scheduler

import (
	"math/rand"

	schedulerv1 "agentenv/services/api/proto"
)

// startingSandboxLoadPercent is the load a sandbox still booting adds to its
// node's score. A boot is the most CPU- and IO-hungry moment of a microVM, and
// the heartbeat that reports it lags by a few seconds, so a node that just
// accepted sandboxes must look busier than its last CPU sample says.
const startingSandboxLoadPercent uint32 = 15

// unknownLoadScore ranks a node without a heartbeat behind every node with
// one: its load cannot be read, so it only wins when both candidates are unknown.
const unknownLoadScore uint32 = ^uint32(0)

// LeastLoadedStrategy picks the less loaded of two distinct nodes sampled at
// random ("power of two choices"). Comparing two random candidates instead of
// always taking the global minimum keeps the placement spread out while the
// heartbeat is stale: every scheduler decision in the same window would
// otherwise pile onto the one node that looked emptiest.
type LeastLoadedStrategy struct {
	pick func(n int) int
}

func NewLeastLoadedStrategy() *LeastLoadedStrategy {
	return newLeastLoadedStrategy(rand.Intn)
}

func newLeastLoadedStrategy(pick func(n int) int) *LeastLoadedStrategy {
	return &LeastLoadedStrategy{pick: pick}
}

func (s *LeastLoadedStrategy) Select(nodes []RichNode, _ *schedulerv1.ScheduleRequestHint) (RichNode, error) {
	switch len(nodes) {
	case 0:
		return RichNode{}, ErrNoNodes
	case 1:
		return nodes[0], nil
	}
	first := s.pick(len(nodes))
	second := s.pick(len(nodes) - 1)
	if second >= first {
		second++
	}
	if nodeLoadScore(nodes[second]) < nodeLoadScore(nodes[first]) {
		return nodes[second], nil
	}
	return nodes[first], nil
}

func (s *LeastLoadedStrategy) Name() string {
	return "least_loaded"
}

// nodeLoadScore is the higher of the node's CPU and memory pressure, in
// percent, plus a fixed share per sandbox still starting there.
func nodeLoadScore(n RichNode) uint32 {
	snapshot := n.Snapshot
	if snapshot == nil {
		return unknownLoadScore
	}
	pressure := snapshot.GetCpuPercent()
	if total := snapshot.GetMemoryTotalBytes(); total > 0 {
		if memoryPercent := uint32(snapshot.GetMemoryUsedBytes() * 100 / total); memoryPercent > pressure {
			pressure = memoryPercent
		}
	}
	return pressure + startingSandboxLoadPercent*snapshot.GetSandboxStartingCount()
}
