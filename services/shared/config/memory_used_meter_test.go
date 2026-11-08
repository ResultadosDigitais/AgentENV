package config

import "testing"

func TestMemoryUsedMeterDefaultsToRaw(t *testing.T) {
	var nilLimit *NodeResourceLimit
	if got := nilLimit.ResolvedMemoryUsedMeter(); got != MemoryUsedMeterRaw {
		t.Fatalf("nil limit memory_used_meter = %q, want raw", got)
	}
	if got := (&NodeResourceLimit{}).ResolvedMemoryUsedMeter(); got != MemoryUsedMeterRaw {
		t.Fatalf("absent memory_used_meter = %q, want raw", got)
	}
}

func TestMemoryUsedMeterResolvesWorkingSet(t *testing.T) {
	meter := " Working_Set "
	if got := (&NodeResourceLimit{MemoryUsedMeter: &meter}).ResolvedMemoryUsedMeter(); got != MemoryUsedMeterWorkingSet {
		t.Fatalf("memory_used_meter = %q, want working_set", got)
	}
}

func TestMemoryUsedMeterUnknownFailsSafeToRaw(t *testing.T) {
	meter := "rss"
	if got := (&NodeResourceLimit{MemoryUsedMeter: &meter}).ResolvedMemoryUsedMeter(); got != MemoryUsedMeterRaw {
		t.Fatalf("unknown memory_used_meter must resolve to raw, got %q", got)
	}
}

func TestMemoryUsedMeterValidate(t *testing.T) {
	str := func(v string) *string { return &v }
	for _, meter := range []string{"raw", "working_set", " RAW "} {
		if err := (&NodeResourceLimit{MemoryUsedMeter: str(meter)}).Validate(); err != nil {
			t.Fatalf("memory_used_meter %q rejected: %v", meter, err)
		}
	}
	for _, meter := range []string{"rss", "", "working-set"} {
		if err := (&NodeResourceLimit{MemoryUsedMeter: str(meter)}).Validate(); err == nil {
			t.Fatalf("memory_used_meter %q accepted", meter)
		}
	}
}

func TestLoadRejectsUnknownMemoryUsedMeter(t *testing.T) {
	if _, err := loadSchedulerConfig(t, `{
		"scheduler": {
			"nodes": [{"id": "node-a", "endpoint": "http://node-a:8000"}],
			"node_resource_limit": {"memory_used_meter": "rss"}
		}
	}`); err == nil {
		t.Fatal("unknown memory_used_meter must be rejected at startup")
	}
}

func TestLoadParsesMemoryUsedMeter(t *testing.T) {
	cfg, err := loadSchedulerConfig(t, `{
		"scheduler": {
			"nodes": [{"id": "node-a", "endpoint": "http://node-a:8000"}],
			"node_resource_limit": {"max_memory_used_percent": 85, "memory_used_meter": "working_set"}
		}
	}`)
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if got := cfg.Scheduler.NodeResourceLimit.ResolvedMemoryUsedMeter(); got != MemoryUsedMeterWorkingSet {
		t.Fatalf("memory_used_meter = %q, want working_set", got)
	}
}
