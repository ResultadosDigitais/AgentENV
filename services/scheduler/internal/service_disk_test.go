package scheduler

import (
	"context"
	"testing"

	schedulerv1 "agentenv/services/api/proto"
	"agentenv/services/shared/config"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func newObservedService(t *testing.T, limit *config.NodeResourceLimit) (*Service, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zap.InfoLevel)
	service := NewService(
		zap.New(core),
		NewAtomicNodeRegistry([]Node{{ID: "pp2j", Endpoint: "http://pp2j"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
		WithNodeResourceLimit(limit),
	)
	if _, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "pp2j",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-pp2j",
		Version:           "0.1.0",
		Commit:            "abc123",
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
			ReportedAtUnixMs: 123,
			Disks: []*schedulerv1.DiskMetric{
				{MountPoint: "/workspace", UsedBytes: 660 * gib, TotalBytes: 737 * gib},
			},
		},
	}); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}
	return service, logs
}

// A disk refusal must be readable in the scheduler log by its limit name, not
// only inferable from the eligible-node count.
func TestScheduleLogsDiskRejectionWithNamedLimit(t *testing.T) {
	service, logs := newObservedService(t, &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(79)})

	_, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
	if err == nil {
		t.Fatal("expected the only node to be rejected")
	}

	entries := logs.FilterMessage("scheduler rejected node by resource limit").All()
	if len(entries) != 1 {
		t.Fatalf("expected 1 rejection log line, got %d", len(entries))
	}
	fields := entries[0].ContextMap()
	if got := fields["node_id"]; got != "pp2j" {
		t.Fatalf("expected node_id pp2j, got %v", got)
	}
	if got := fields["limit"]; got != "max_disk_used_percent" {
		t.Fatalf("expected limit max_disk_used_percent, got %v", got)
	}
	if got := fields["observed"]; got != "89" {
		t.Fatalf("expected observed 89, got %v", got)
	}
	if got := fields["threshold"]; got != "79" {
		t.Fatalf("expected threshold 79, got %v", got)
	}
	if got := fields["detail"]; got != "mount_point=/workspace" {
		t.Fatalf("expected the mount point detail, got %v", got)
	}
}

// The same line names the pre-existing limits, so the log format is uniform
// across every limit.
func TestScheduleLogsMemoryRejectionWithNamedLimit(t *testing.T) {
	core, logs := observer.New(zap.InfoLevel)
	service := NewService(
		zap.New(core),
		NewAtomicNodeRegistry([]Node{{ID: "node-a", Endpoint: "http://node-a"}}, defaultObservedReportTTL),
		NewStrategy("round_robin"),
		NewInMemoryBindingStore(defaultObservedReportTTL),
		WithNodeResourceLimit(&config.NodeResourceLimit{MaxMemoryUsedPercent: uint32Ptr(85)}),
	)
	if _, err := service.Heartbeat(context.Background(), &schedulerv1.HeartbeatRequest{
		NodeId:            "node-a",
		ClusterId:         "cluster-1",
		ServiceInstanceId: "svc-a",
		Version:           "0.1.0",
		Commit:            "abc123",
		Snapshot: &schedulerv1.NodeSnapshot{
			Status:           schedulerv1.NodeStatus_NODE_STATUS_READY,
			ReportedAtUnixMs: 123,
			MemoryUsedBytes:  90 * gib,
			MemoryTotalBytes: 100 * gib,
		},
	}); err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	if _, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{}); err == nil {
		t.Fatal("expected the only node to be rejected")
	}
	entries := logs.FilterMessage("scheduler rejected node by resource limit").All()
	if len(entries) != 1 {
		t.Fatalf("expected 1 rejection log line, got %d", len(entries))
	}
	if got := entries[0].ContextMap()["limit"]; got != "max_memory_used_percent" {
		t.Fatalf("expected limit max_memory_used_percent, got %v", got)
	}
}

// An admitted node must not produce a rejection line.
func TestScheduleDoesNotLogAdmittedNodeAsRejected(t *testing.T) {
	service, logs := newObservedService(t, &config.NodeResourceLimit{MaxDiskUsedPercent: uint32Ptr(90)})

	resp, err := service.Schedule(context.Background(), &schedulerv1.ScheduleRequest{})
	if err != nil {
		t.Fatalf("schedule failed: %v", err)
	}
	if got := resp.GetNode().GetNodeId(); got != "pp2j" {
		t.Fatalf("expected pp2j, got %q", got)
	}
	if entries := logs.FilterMessage("scheduler rejected node by resource limit").All(); len(entries) != 0 {
		t.Fatalf("expected no rejection log line, got %d", len(entries))
	}
}
