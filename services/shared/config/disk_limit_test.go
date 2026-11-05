package config

import (
	"os"
	"path/filepath"
	"testing"
)

func loadSchedulerConfig(t *testing.T, content string) (Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write config file failed: %v", err)
	}
	return Load(path, "scheduler")
}

func uint32Ref(v uint32) *uint32 { return &v }

func TestLoadParsesSchedulerDiskLimit(t *testing.T) {
	cfg, err := loadSchedulerConfig(t, `{
		"scheduler": {
			"nodes": [{"id": "node-a", "endpoint": "http://node-a:8000"}],
			"node_resource_limit": {
				"max_disk_used_percent": 79,
				"disk_mount_point": "/workspace"
			}
		}
	}`)
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	limit := cfg.Scheduler.NodeResourceLimit
	if limit == nil {
		t.Fatal("expected node_resource_limit to be parsed")
	}
	if limit.MaxDiskUsedPercent == nil || *limit.MaxDiskUsedPercent != 79 {
		t.Fatalf("expected max_disk_used_percent 79, got %v", limit.MaxDiskUsedPercent)
	}
	if got := limit.ResolvedDiskMountPoint(); got != "/workspace" {
		t.Fatalf("expected disk_mount_point /workspace, got %q", got)
	}
}

func TestSchedulerDiskMountPointDefaultsToWorkspace(t *testing.T) {
	var nilLimit *NodeResourceLimit
	if got := nilLimit.ResolvedDiskMountPoint(); got != DefaultDiskMountPoint {
		t.Fatalf("nil limit must resolve to %q, got %q", DefaultDiskMountPoint, got)
	}
	if got := (&NodeResourceLimit{}).ResolvedDiskMountPoint(); got != DefaultDiskMountPoint {
		t.Fatalf("absent mount point must resolve to %q, got %q", DefaultDiskMountPoint, got)
	}
	if DefaultDiskMountPoint != "/workspace" {
		t.Fatalf("expected the default mount point to be /workspace, got %q", DefaultDiskMountPoint)
	}
	blank := "  "
	if got := (&NodeResourceLimit{DiskMountPoint: &blank}).ResolvedDiskMountPoint(); got != DefaultDiskMountPoint {
		t.Fatalf("blank mount point must resolve to %q, got %q", DefaultDiskMountPoint, got)
	}
}

// Omitting max_disk_used_percent must impose no disk limit at all.
func TestSchedulerDiskLimitAbsentMeansNoLimit(t *testing.T) {
	cfg, err := loadSchedulerConfig(t, `{
		"scheduler": {
			"nodes": [{"id": "node-a", "endpoint": "http://node-a:8000"}],
			"node_resource_limit": {"max_cpu_used_percent": 90}
		}
	}`)
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Scheduler.NodeResourceLimit.MaxDiskUsedPercent != nil {
		t.Fatalf("expected no disk limit, got %v", *cfg.Scheduler.NodeResourceLimit.MaxDiskUsedPercent)
	}

	cfg, err = loadSchedulerConfig(t, `{
		"scheduler": {
			"nodes": [{"id": "node-a", "endpoint": "http://node-a:8000"}]
		}
	}`)
	if err != nil {
		t.Fatalf("load config without node_resource_limit failed: %v", err)
	}
	if cfg.Scheduler.NodeResourceLimit != nil {
		t.Fatalf("expected no node_resource_limit, got %+v", cfg.Scheduler.NodeResourceLimit)
	}
}

func TestSchedulerDiskLimitRejectsPercentAbove100(t *testing.T) {
	_, err := loadSchedulerConfig(t, `{
		"scheduler": {
			"nodes": [{"id": "node-a", "endpoint": "http://node-a:8000"}],
			"node_resource_limit": {"max_disk_used_percent": 101}
		}
	}`)
	if err == nil {
		t.Fatal("expected max_disk_used_percent 101 to be rejected")
	}
}

func TestSchedulerDiskLimitAcceptsBoundaryPercentages(t *testing.T) {
	for _, percent := range []uint32{0, 1, 79, 100} {
		if err := (&NodeResourceLimit{MaxDiskUsedPercent: uint32Ref(percent)}).Validate(); err != nil {
			t.Fatalf("max_disk_used_percent %d should validate: %v", percent, err)
		}
	}
}

// The disk percentage is a real usage ratio: unlike the allocated limits it
// can never legitimately exceed 100.
func TestSchedulerDiskLimitValidateNamesTheField(t *testing.T) {
	err := (&NodeResourceLimit{MaxDiskUsedPercent: uint32Ref(150)}).Validate()
	if err == nil {
		t.Fatal("expected max_disk_used_percent 150 to be rejected")
	}
	want := "scheduler.node_resource_limit.max_disk_used_percent must be at most 100, got 150"
	if err.Error() != want {
		t.Fatalf("expected %q, got %q", want, err.Error())
	}
}
