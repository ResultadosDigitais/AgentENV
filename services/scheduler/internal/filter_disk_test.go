package scheduler

import (
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

const workspaceMount = "/workspace"

func diskMetric(mountPoint string, usedBytes, totalBytes uint64) *schedulerv1.DiskMetric {
	return &schedulerv1.DiskMetric{
		MountPoint: mountPoint,
		UsedBytes:  usedBytes,
		TotalBytes: totalBytes,
	}
}

func diskNode(id string, disks ...*schedulerv1.DiskMetric) RichNode {
	return RichNode{Node: Node{ID: id}, Snapshot: &schedulerv1.NodeSnapshot{Disks: disks}}
}

// diskOutcomeCount reads the counter straight from client_model, which is
// already an indirect module dependency; prometheus/testutil would add one.
func diskOutcomeCount(t *testing.T, outcome string) float64 {
	t.Helper()
	counter, ok := schedulerDiskAdmission.WithLabelValues(outcome).(prometheus.Counter)
	if !ok {
		t.Fatalf("schedulerDiskAdmission entry %q is not a counter", outcome)
	}
	var metric dto.Metric
	if err := counter.Write(&metric); err != nil {
		t.Fatalf("read counter %q: %v", outcome, err)
	}
	return metric.GetCounter().GetValue()
}

// A node whose target mount is above max_disk_used_percent is dropped from the
// candidate list.
func TestDiskAdmissionRejectsNodeAboveLimit(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79)}
	// The pp2j shape from OPSAI-6108: 660 of 737 GB used, 89% with 77 GB free.
	nodes := []RichNode{diskNode("pp2j", diskMetric(workspaceMount, 660*gib, 737*gib))}
	sameIDs(t, FilterByResourceLimit(nodes, limit))
}

// Negative control: the same node below the limit is still admitted.
func TestDiskAdmissionAdmitsNodeBelowLimit(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79)}
	nodes := []RichNode{
		diskNode("roomy", diskMetric(workspaceMount, 500*gib, 737*gib)), // 67%
		diskNode("pp2j", diskMetric(workspaceMount, 660*gib, 737*gib)),
	}
	sameIDs(t, FilterByResourceLimit(nodes, limit), "roomy")
}

// The convention shared with the other limits is a strict greater-than, so a
// node sitting exactly on the limit is admitted.
func TestDiskAdmissionAdmitsNodeExactlyAtLimit(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79)}
	nodes := []RichNode{diskNode("at-limit", diskMetric(workspaceMount, 79, 100))}
	sameIDs(t, FilterByResourceLimit(nodes, limit), "at-limit")
}

// Only the configured mount point is compared. A full tmpfs or the subPath
// bind mount of the pod's config file must not reject the node.
func TestDiskAdmissionIgnoresOtherMounts(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79)}
	nodes := []RichNode{
		diskNode("irrelevant-mount-over",
			diskMetric(workspaceMount, 100*gib, 737*gib),
			diskMetric("/workspace/config/agentenv.toml", 1*gib, 1*gib),
			diskMetric("/run", 64*gib, 64*gib),
		),
	}
	sameIDs(t, FilterByResourceLimit(nodes, limit), "irrelevant-mount-over")
}

// A node whose heartbeat does not report the target mount is admitted
// (fail-open) and counted, so an inert limit is visible instead of silent.
func TestDiskAdmissionFailsOpenWhenTargetMountIsMissing(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79)}
	nodes := []RichNode{
		diskNode("old-agent", diskMetric("/", 900*gib, 1000*gib)),
		diskNode("no-disks"),
	}
	before := diskOutcomeCount(t, "mount_missing")
	sameIDs(t, FilterByResourceLimit(nodes, limit), "old-agent", "no-disks")
	if got := diskOutcomeCount(t, "mount_missing") - before; got != 2 {
		t.Fatalf("expected 2 mount_missing outcomes, got %v", got)
	}
}

// The fail-open decision is named for the log: it points at the disk limit,
// the mount it looked for and why it admitted the node, and it stays silent
// for a node the disk limit could actually evaluate.
func TestDiskLimitSkippedNamesTheMissingMount(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79)}

	missing, ok := diskLimitSkipped(&schedulerv1.NodeSnapshot{Disks: []*schedulerv1.DiskMetric{
		diskMetric("/", 900*gib, 1000*gib),
	}}, limit)
	if !ok {
		t.Fatal("expected the disk limit to be reported as skipped")
	}
	if missing.Limit != limitMaxDiskUsedPercent {
		t.Fatalf("expected limit %q, got %q", limitMaxDiskUsedPercent, missing.Limit)
	}
	if missing.Threshold != "79" {
		t.Fatalf("expected threshold 79, got %q", missing.Threshold)
	}
	if missing.Detail != "mount_point=/workspace reason=mount_missing" {
		t.Fatalf("unexpected detail %q", missing.Detail)
	}

	if _, ok := diskLimitSkipped(&schedulerv1.NodeSnapshot{Disks: []*schedulerv1.DiskMetric{
		diskMetric(workspaceMount, 500*gib, 737*gib),
	}}, limit); ok {
		t.Fatal("expected a measurable mount not to be reported as skipped")
	}
	if _, ok := diskLimitSkipped(&schedulerv1.NodeSnapshot{
		Disks: []*schedulerv1.DiskMetric{diskMetric(workspaceMount, 5, 0)},
	}, limit); !ok {
		t.Fatal("expected an unmeasurable mount to be reported as skipped")
	}
	if _, ok := diskLimitSkipped(&schedulerv1.NodeSnapshot{}, &config.NodeResourceLimit{}); ok {
		t.Fatal("expected no skip report when the disk limit is not configured")
	}
}

// A target mount reporting total_bytes = 0 cannot be measured and is admitted.
func TestDiskAdmissionAdmitsUnmeasurableMount(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(1)}
	nodes := []RichNode{diskNode("zero-total", diskMetric(workspaceMount, 5, 0))}
	sameIDs(t, FilterByResourceLimit(nodes, limit), "zero-total")
}

// The disk check is off unless max_disk_used_percent is configured, and a nil
// limit keeps disabling every check.
func TestDiskAdmissionDisabledWithoutLimit(t *testing.T) {
	full := diskNode("full", diskMetric(workspaceMount, 737*gib, 737*gib))

	sameIDs(t, FilterByResourceLimit([]RichNode{full}, &config.NodeResourceLimit{}), "full")
	sameIDs(t, FilterByResourceLimit([]RichNode{full}, nil), "full")
}

// A custom disk_mount_point selects that mount instead of the /workspace
// default.
func TestDiskAdmissionUsesConfiguredMountPoint(t *testing.T) {
	mount := "/data"
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79), DiskMountPoint: &mount}
	nodes := []RichNode{
		diskNode("data-over",
			diskMetric(workspaceMount, 10*gib, 737*gib),
			diskMetric(mount, 700*gib, 737*gib),
		),
		diskNode("workspace-over",
			diskMetric(workspaceMount, 700*gib, 737*gib),
			diskMetric(mount, 10*gib, 737*gib),
		),
	}
	sameIDs(t, FilterByResourceLimit(nodes, limit), "workspace-over")
}

// The disk counter classifies every node that has a heartbeat exactly once,
// independently of the verdict reached by the other limits.
func TestDiskAdmissionCountsEveryEvaluatedNode(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79)}
	nodes := []RichNode{
		diskNode("under", diskMetric(workspaceMount, 500*gib, 737*gib)),
		diskNode("over", diskMetric(workspaceMount, 660*gib, 737*gib)),
		diskNode("missing", diskMetric("/", 1*gib, 2*gib)),
		{Node: Node{ID: "no-heartbeat"}},
	}
	within := diskOutcomeCount(t, "within_limit")
	over := diskOutcomeCount(t, "over_limit")
	missing := diskOutcomeCount(t, "mount_missing")
	FilterByResourceLimit(nodes, limit)
	if got := diskOutcomeCount(t, "within_limit") - within; got != 1 {
		t.Fatalf("expected 1 within_limit outcome, got %v", got)
	}
	if got := diskOutcomeCount(t, "over_limit") - over; got != 1 {
		t.Fatalf("expected 1 over_limit outcome, got %v", got)
	}
	if got := diskOutcomeCount(t, "mount_missing") - missing; got != 1 {
		t.Fatalf("expected 1 mount_missing outcome, got %v", got)
	}
}

// The named reason is what the scheduler log carries: the disk limit names
// max_disk_used_percent, and the pre-existing limits keep naming their own key.
func TestLimitViolationNamesTheExceededLimit(t *testing.T) {
	total := 100 * gib
	limit := &config.NodeResourceLimit{
		MaxDiskUsedPercent:      uint32Ptr(79),
		MaxCPUUsedPercent:       uint32Ptr(90),
		MaxMemoryUsedPercent:    uint32Ptr(85),
		MaxSandboxCount:         uint32Ptr(50),
		MaxSandboxStartingCount: uint32Ptr(10),
	}
	cases := []struct {
		name string
		node RichNode
		want string
	}{
		{
			name: "disk",
			node: diskNode("n", diskMetric(workspaceMount, 90*gib, total)),
			want: limitMaxDiskUsedPercent,
		},
		{
			name: "cpu-used",
			node: RichNode{Node: Node{ID: "n"}, Snapshot: &schedulerv1.NodeSnapshot{CpuPercent: 95}},
			want: limitMaxCPUUsedPercent,
		},
		{
			name: "memory-used",
			node: memNode("n", total, 0, 0, 90*gib),
			want: limitMaxMemoryUsedPercent,
		},
		{
			name: "sandbox-count",
			node: RichNode{Node: Node{ID: "n"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxCount: 60}},
			want: limitMaxSandboxCount,
		},
		{
			name: "sandbox-starting",
			node: RichNode{Node: Node{ID: "n"}, Snapshot: &schedulerv1.NodeSnapshot{SandboxStartingCount: 11}},
			want: limitMaxSandboxStartingCount,
		},
	}
	for _, tc := range cases {
		violation, violated := limitViolated(tc.node, limit)
		if !violated {
			t.Fatalf("%s: expected the node to be rejected", tc.name)
		}
		if violation.Limit != tc.want {
			t.Fatalf("%s: expected limit %q, got %q", tc.name, tc.want, violation.Limit)
		}
		if violation.Observed == "" || violation.Threshold == "" {
			t.Fatalf("%s: expected observed and threshold, got %+v", tc.name, violation)
		}
	}
}

// The rejection observer receives the named reason for a disk refusal, which
// is what the scheduler logs.
func TestDiskRejectionIsReportedToObserver(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79)}
	nodes := []RichNode{diskNode("pp2j", diskMetric(workspaceMount, 660*gib, 737*gib))}

	var got []LimitViolation
	FilterByResourceLimitRejected(nodes, limit, nil, func(nodeID string, violation LimitViolation) {
		if nodeID != "pp2j" {
			t.Fatalf("unexpected node id %q", nodeID)
		}
		got = append(got, violation)
	})
	if len(got) != 1 {
		t.Fatalf("expected 1 rejection, got %d", len(got))
	}
	if got[0].Limit != limitMaxDiskUsedPercent {
		t.Fatalf("expected limit %q, got %q", limitMaxDiskUsedPercent, got[0].Limit)
	}
	if got[0].Observed != "89" || got[0].Threshold != "79" {
		t.Fatalf("expected observed 89 and threshold 79, got %+v", got[0])
	}
	if got[0].Detail != "mount_point=/workspace" {
		t.Fatalf("expected the mount point in the detail, got %q", got[0].Detail)
	}
}

// Admitted nodes produce no rejection line.
func TestAdmittedNodeIsNotReportedAsRejected(t *testing.T) {
	limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79)}
	nodes := []RichNode{diskNode("roomy", diskMetric(workspaceMount, 500*gib, 737*gib))}

	calls := 0
	FilterByResourceLimitRejected(nodes, limit, nil, func(string, LimitViolation) { calls++ })
	if calls != 0 {
		t.Fatalf("expected no rejection, got %d", calls)
	}
}

// The disk limit is always effective: admission_mode only changes the memory
// verdict, never the disk one.
func TestDiskAdmissionIgnoresAdmissionMode(t *testing.T) {
	for _, mode := range []string{"nominal", "request", "shadow"} {
		m := mode
		limit := &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79), AdmissionMode: &m}
		nodes := []RichNode{diskNode("over", diskMetric(workspaceMount, 660*gib, 737*gib))}
		sameIDs(t, FilterByResourceLimit(nodes, limit))
	}
}
