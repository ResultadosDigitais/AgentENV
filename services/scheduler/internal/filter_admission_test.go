package scheduler

import (
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"
)

const gib = uint64(1) << 30

func strPtr(v string) *string     { return &v }
func floatPtr(v float64) *float64 { return &v }

func memNode(id string, total, allocated, requested, used uint64) RichNode {
	return RichNode{Node: Node{ID: id}, Snapshot: &schedulerv1.NodeSnapshot{
		MemoryTotalBytes:     total,
		AllocatedMemoryBytes: allocated,
		RequestedMemoryBytes: requested,
		MemoryUsedBytes:      used,
	}}
}

func sameIDs(t *testing.T, got []RichNode, want ...string) {
	t.Helper()
	g := make([]string, 0, len(got))
	for _, n := range got {
		g = append(g, n.ID)
	}
	if len(g) != len(want) {
		t.Fatalf("got %v, want %v", g, want)
	}
	for i := range g {
		if g[i] != want[i] {
			t.Fatalf("got %v, want %v", g, want)
		}
	}
}

// Without admission_mode the decision is the historical one:
// requested_memory_bytes and the overcommit ratio are ignored.
func TestAdmissionDefaultModeIsNominalAndIgnoresRequestAndRatio(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent:     uint32Ptr(65),
		MaxMemoryLimitOvercommitRatio: floatPtr(0.1), // would reject everything in request mode
	}
	nodes := []RichNode{
		memNode("nominal-ok", 100*gib, 60*gib, 1*gib, 0),
		memNode("nominal-over", 100*gib, 70*gib, 1*gib, 0),
	}
	sameIDs(t, FilterByResourceLimit(nodes, limit), "nominal-ok")

	limit.AdmissionMode = strPtr("nominal")
	sameIDs(t, FilterByResourceLimit(nodes, limit), "nominal-ok")

	// Unknown value resolves to nominal at runtime (config.Validate rejects it at startup).
	limit.AdmissionMode = strPtr("bogus")
	sameIDs(t, FilterByResourceLimit(nodes, limit), "nominal-ok")
}

func TestAdmissionRequestModeCountsRequests(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(65),
		AdmissionMode:             strPtr("request"),
	}
	nodes := []RichNode{
		// nominal 120% (over 65) but requests only 40%; limits within default ratio 1.5.
		memNode("admit-by-request", 100*gib, 120*gib, 40*gib, 0),
		// requests 70% > 65.
		memNode("request-over", 100*gib, 120*gib, 70*gib, 0),
	}
	sameIDs(t, FilterByResourceLimit(nodes, limit), "admit-by-request")
}

func TestAdmissionRequestModeLimitOvercommitRatio(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(65),
		AdmissionMode:             strPtr("request"),
	}
	nodes := []RichNode{
		memNode("at-default-ratio", 100*gib, 150*gib, 10*gib, 0), // 1.5x exactly, kept
		memNode("over-default-ratio", 100*gib, 151*gib, 10*gib, 0),
	}
	sameIDs(t, FilterByResourceLimit(nodes, limit), "at-default-ratio")

	limit.MaxMemoryLimitOvercommitRatio = floatPtr(2.0)
	sameIDs(t, FilterByResourceLimit(nodes, limit), "at-default-ratio", "over-default-ratio")

	limit.MaxMemoryLimitOvercommitRatio = floatPtr(1.0)
	sameIDs(t, FilterByResourceLimit(nodes, limit))
}

// Real host usage stays the backstop in every mode.
func TestAdmissionUsedPercentBackstopAppliesInRequestMode(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(65),
		MaxMemoryUsedPercent:      uint32Ptr(85),
		AdmissionMode:             strPtr("request"),
	}
	nodes := []RichNode{
		memNode("low-use", 100*gib, 100*gib, 10*gib, 50*gib),
		memNode("high-use", 100*gib, 100*gib, 10*gib, 90*gib),
	}
	sameIDs(t, FilterByResourceLimit(nodes, limit), "low-use")
}

// Nodes running an older agent do not report requested_memory_bytes.
func TestAdmissionRequestModeFallsBackToNominalForNonReportingNodes(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(65),
		AdmissionMode:             strPtr("request"),
	}
	nodes := []RichNode{
		memNode("old-agent-over", 100*gib, 70*gib, 0, 0),
		memNode("old-agent-ok", 100*gib, 60*gib, 0, 0),
		memNode("empty-node", 100*gib, 0, 0, 0),
	}
	sameIDs(t, FilterByResourceLimit(nodes, limit), "old-agent-ok", "empty-node")
}

type shadowEvent struct {
	id               string
	nominal, request bool
}

func shadowRun(nodes []RichNode, limit *config.NodeResourceLimit) ([]RichNode, []shadowEvent) {
	var events []shadowEvent
	got := FilterByResourceLimitObserved(nodes, limit, func(id string, nominal, request bool, _ string) {
		events = append(events, shadowEvent{id, nominal, request})
	})
	return got, events
}

// Shadow mode decides with nominal figures; the observer fires only when the
// hypothetical request decision differs.
func TestAdmissionShadowDecidesNominalAndReportsRequestWouldAdmit(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent: uint32Ptr(65),
		AdmissionMode:             strPtr("shadow"),
	}
	nodes := []RichNode{
		memNode("agree-admit", 100*gib, 50*gib, 50*gib, 0),
		memNode("agree-reject", 100*gib, 160*gib, 160*gib, 0),
		memNode("request-would-admit", 100*gib, 120*gib, 40*gib, 0),
	}
	got, events := shadowRun(nodes, limit)
	sameIDs(t, got, "agree-admit") // effective decision is nominal
	if len(events) != 1 || events[0] != (shadowEvent{"request-would-admit", false, true}) {
		t.Fatalf("unexpected divergence events: %+v", events)
	}
}

func TestAdmissionShadowReportsRequestWouldReject(t *testing.T) {
	limit := &config.NodeResourceLimit{
		MaxMemoryAllocatedPercent:     uint32Ptr(65),
		MaxMemoryLimitOvercommitRatio: floatPtr(0.5),
		AdmissionMode:                 strPtr("shadow"),
	}
	// nominal admits (60% <= 65) but the nominal-limit ratio cap (0.5) would reject in request mode.
	nodes := []RichNode{memNode("n", 100*gib, 60*gib, 10*gib, 0)}
	got, events := shadowRun(nodes, limit)
	sameIDs(t, got, "n")
	if len(events) != 1 || events[0] != (shadowEvent{"n", true, false}) {
		t.Fatalf("unexpected divergence events: %+v", events)
	}
}

func TestAdmissionNonShadowModesNeverNotifyObserver(t *testing.T) {
	for _, mode := range []string{"nominal", "request"} {
		limit := &config.NodeResourceLimit{
			MaxMemoryAllocatedPercent: uint32Ptr(65),
			AdmissionMode:             strPtr(mode),
		}
		_, events := shadowRun([]RichNode{memNode("n", 100*gib, 120*gib, 40*gib, 0)}, limit)
		if len(events) != 0 {
			t.Fatalf("mode %s must not notify: %+v", mode, events)
		}
	}
}

func TestAdmissionNodesWithoutSnapshotOrTotalAreKeptInAllModes(t *testing.T) {
	for _, mode := range []string{"nominal", "request", "shadow"} {
		limit := &config.NodeResourceLimit{
			MaxMemoryAllocatedPercent: uint32Ptr(1),
			AdmissionMode:             strPtr(mode),
		}
		nodes := []RichNode{
			{Node: Node{ID: "no-snapshot"}},
			memNode("no-total", 0, 10*gib, 10*gib, 0),
		}
		sameIDs(t, FilterByResourceLimit(nodes, limit), "no-snapshot", "no-total")
	}
}
