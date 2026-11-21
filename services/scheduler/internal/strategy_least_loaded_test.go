package scheduler

import (
	"testing"

	schedulerv1 "agentenv/services/api/proto"
)

// sequencePicker returns the given indexes in order, so the two sampled
// candidates of each Select are deterministic.
func sequencePicker(picks ...int) func(int) int {
	i := 0
	return func(n int) int {
		p := picks[i%len(picks)] % n
		i++
		return p
	}
}

func snap(cpuPercent uint32, memUsed, memTotal uint64, starting uint32) *schedulerv1.NodeSnapshot {
	return &schedulerv1.NodeSnapshot{
		CpuPercent:           cpuPercent,
		MemoryUsedBytes:      memUsed,
		MemoryTotalBytes:     memTotal,
		SandboxStartingCount: starting,
	}
}

func TestLeastLoadedPicksTheLessLoadedOfTheTwoSampled(t *testing.T) {
	s := newLeastLoadedStrategy(sequencePicker(0, 0))
	nodes := []RichNode{
		{Node: Node{ID: "hot"}, Snapshot: snap(93, 40, 100, 0)},
		{Node: Node{ID: "cold"}, Snapshot: snap(9, 10, 100, 0)},
	}
	got, err := s.Select(nodes, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != "cold" {
		t.Fatalf("expected cold, got %s", got.ID)
	}
}

func TestLeastLoadedUsesMemoryWhenItIsTheHigherPressure(t *testing.T) {
	s := newLeastLoadedStrategy(sequencePicker(0, 0))
	nodes := []RichNode{
		{Node: Node{ID: "memory-full"}, Snapshot: snap(10, 95, 100, 0)},
		{Node: Node{ID: "busy-cpu"}, Snapshot: snap(40, 30, 100, 0)},
	}
	got, _ := s.Select(nodes, nil)
	if got.ID != "busy-cpu" {
		t.Fatalf("expected busy-cpu, got %s", got.ID)
	}
}

func TestLeastLoadedCountsSandboxesStillStarting(t *testing.T) {
	s := newLeastLoadedStrategy(sequencePicker(0, 0))
	nodes := []RichNode{
		{Node: Node{ID: "starting"}, Snapshot: snap(20, 20, 100, 3)},
		{Node: Node{ID: "idle"}, Snapshot: snap(30, 30, 100, 0)},
	}
	got, _ := s.Select(nodes, nil)
	if got.ID != "idle" {
		t.Fatalf("expected idle, got %s", got.ID)
	}
}

func TestLeastLoadedPrefersANodeWithAHeartbeat(t *testing.T) {
	s := newLeastLoadedStrategy(sequencePicker(0, 0))
	nodes := []RichNode{
		{Node: Node{ID: "unknown"}},
		{Node: Node{ID: "known"}, Snapshot: snap(70, 70, 100, 0)},
	}
	got, _ := s.Select(nodes, nil)
	if got.ID != "known" {
		t.Fatalf("expected known, got %s", got.ID)
	}
}

func TestLeastLoadedSamplesTwoDistinctNodes(t *testing.T) {
	// Both picks return 1: the second candidate must still differ from the first.
	s := newLeastLoadedStrategy(sequencePicker(1, 1))
	nodes := []RichNode{
		{Node: Node{ID: "a"}, Snapshot: snap(10, 10, 100, 0)},
		{Node: Node{ID: "b"}, Snapshot: snap(50, 50, 100, 0)},
		{Node: Node{ID: "c"}, Snapshot: snap(90, 90, 100, 0)},
	}
	got, _ := s.Select(nodes, nil)
	if got.ID != "b" {
		t.Fatalf("expected b (candidates b and c), got %s", got.ID)
	}
}

func TestLeastLoadedSingleNode(t *testing.T) {
	s := newLeastLoadedStrategy(sequencePicker(0))
	nodes := []RichNode{{Node: Node{ID: "only"}, Snapshot: snap(99, 99, 100, 4)}}
	got, err := s.Select(nodes, nil)
	if err != nil || got.ID != "only" {
		t.Fatalf("expected only, got %s (%v)", got.ID, err)
	}
}

func TestLeastLoadedNoNodes(t *testing.T) {
	s := NewLeastLoadedStrategy()
	if _, err := s.Select(nil, nil); err != ErrNoNodes {
		t.Fatalf("expected ErrNoNodes, got %v", err)
	}
}

func TestNewStrategyResolvesLeastLoaded(t *testing.T) {
	if got := NewStrategy("least_loaded").Name(); got != "least_loaded" {
		t.Fatalf("expected least_loaded, got %s", got)
	}
	if got := schedulerStrategyLabel("least_loaded"); got != "least_loaded" {
		t.Fatalf("expected metrics label least_loaded, got %s", got)
	}
}
