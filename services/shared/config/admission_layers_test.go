package config

import (
	"path/filepath"
	"testing"
)

func TestAdmissionLayerDefaultsAreEightSixteenThirtyTwoSixtyFour(t *testing.T) {
	layers := (&NodeResourceLimit{}).ResolvedAdmissionLayers()
	want := []uint64{8, 16, 32, 64}
	if len(layers) != len(want) {
		t.Fatalf("got %d layers, want %d", len(layers), len(want))
	}
	for i, memoryGB := range want {
		if layers[i].MemoryGB != memoryGB {
			t.Fatalf("layer %d memory_gb = %d, want %d", i, layers[i].MemoryGB, memoryGB)
		}
		if layers[i].CPUCount != nil {
			t.Fatalf("layer %d must not invent a cpu_count", i)
		}
	}
}

func TestAdmissionLayerDefaultsApplyToNilLimit(t *testing.T) {
	var limit *NodeResourceLimit
	if got := len(limit.ResolvedAdmissionLayers()); got != 4 {
		t.Fatalf("nil limit got %d layers, want 4", got)
	}
	if !limit.ShapeAdmissionEnabled() {
		t.Fatal("nil limit must keep shape admission on")
	}
	if limit.ResolvedMemoryCapacitySource() != MemoryCapacitySourceAllocatable {
		t.Fatal("nil limit must resolve to allocatable")
	}
	if limit.CrossNodeResumeEnabled() {
		t.Fatal("nil limit must keep cross-node resume off")
	}
}

func TestAdmissionLayerKillSwitches(t *testing.T) {
	str := func(v string) *string { return &v }
	if got := (&NodeResourceLimit{MemoryCapacitySource: str("total")}).ResolvedMemoryCapacitySource(); got != MemoryCapacitySourceTotal {
		t.Fatalf("memory_capacity_source total = %q", got)
	}
	if got := (&NodeResourceLimit{MemoryCapacitySource: str("bogus")}).ResolvedMemoryCapacitySource(); got != MemoryCapacitySourceAllocatable {
		t.Fatalf("unknown memory_capacity_source must fail safe to allocatable, got %q", got)
	}
	if (&NodeResourceLimit{ShapeAdmission: str("off")}).ShapeAdmissionEnabled() {
		t.Fatal("shape_admission off must disable the verdict")
	}
	if !(&NodeResourceLimit{ShapeAdmission: str("bogus")}).ShapeAdmissionEnabled() {
		t.Fatal("unknown shape_admission must fail safe to on")
	}
	enabled := true
	if !(&NodeResourceLimit{CrossNodeResume: &enabled}).CrossNodeResumeEnabled() {
		t.Fatal("cross_node_resume true must enable it")
	}
}

func TestAdmissionLayersValidate(t *testing.T) {
	str := func(v string) *string { return &v }
	valid := []*NodeResourceLimit{
		{MemoryCapacitySource: str("allocatable")},
		{MemoryCapacitySource: str(" Total ")},
		{ShapeAdmission: str("on")},
		{ShapeAdmission: str(" OFF ")},
		{AdmissionLayers: []AdmissionLayer{{MemoryGB: 8}, {MemoryGB: 16}}},
	}
	for i, limit := range valid {
		if err := limit.Validate(); err != nil {
			t.Fatalf("valid limit %d rejected: %v", i, err)
		}
	}

	invalid := []*NodeResourceLimit{
		{MemoryCapacitySource: str("physical")},
		{ShapeAdmission: str("sometimes")},
		{AdmissionLayers: []AdmissionLayer{{MemoryGB: 0}}},
		{AdmissionLayers: []AdmissionLayer{{MemoryGB: 8}, {MemoryGB: 8}}},
	}
	for i, limit := range invalid {
		if err := limit.Validate(); err == nil {
			t.Fatalf("invalid limit %d accepted", i)
		}
	}
}

func TestLoadParsesAdmissionLayersAndKillSwitches(t *testing.T) {
	cfg, err := loadSchedulerConfig(t, `{
		"scheduler": {
			"nodes": [{"id": "node-a", "endpoint": "http://node-a:8000"}],
			"node_resource_limit": {
				"max_memory_allocated_percent": 80,
				"memory_capacity_source": "allocatable",
				"shape_admission": "on",
				"cross_node_resume": true,
				"admission_layers": [
					{"memory_gb": 8},
					{"memory_gb": 16, "cpu_count": 4},
					{"memory_gb": 32},
					{"memory_gb": 64}
				]
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
	if got := limit.ResolvedMemoryCapacitySource(); got != MemoryCapacitySourceAllocatable {
		t.Fatalf("memory_capacity_source = %q", got)
	}
	if !limit.ShapeAdmissionEnabled() {
		t.Fatal("shape_admission must be on")
	}
	if !limit.CrossNodeResumeEnabled() {
		t.Fatal("cross_node_resume must be true")
	}
	layers := limit.ResolvedAdmissionLayers()
	if len(layers) != 4 {
		t.Fatalf("got %d layers, want 4", len(layers))
	}
	if layers[1].CPUCount == nil || *layers[1].CPUCount != 4 {
		t.Fatalf("layer 16 GiB cpu_count = %v, want 4", layers[1].CPUCount)
	}
}

func TestLoadRejectsInvalidAdmissionLayers(t *testing.T) {
	if _, err := loadSchedulerConfig(t, `{
		"scheduler": {
			"nodes": [{"id": "node-a", "endpoint": "http://node-a:8000"}],
			"node_resource_limit": {"admission_layers": [{"memory_gb": 0}]}
		}
	}`); err == nil {
		t.Fatal("zero memory_gb must be rejected")
	}
}

// OPSAI-6650: shape_admission gained a third mode. "on" decides by the shape
// rule, "off" restores the pre-OPSAI-6200 routing and "shadow" evaluates the
// rule and records a verdict while deciding exactly like "off".
func TestResolvedShapeAdmissionSupportsShadow(t *testing.T) {
	str := func(v string) *string { return &v }
	cases := []struct {
		name     string
		value    *string
		want     string
		enabled  bool
		shadow   bool
		observed bool
	}{
		{"absent defaults on", nil, ShapeAdmissionOn, true, false, true},
		{"on", str("on"), ShapeAdmissionOn, true, false, true},
		{"off", str("off"), ShapeAdmissionOff, false, false, false},
		{"shadow", str("shadow"), ShapeAdmissionShadow, false, true, true},
		{"shadow with spaces and case", str("  SHADOW "), ShapeAdmissionShadow, false, true, true},
		{"unknown fails safe to on", str("sometimes"), ShapeAdmissionOn, true, false, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limit := &NodeResourceLimit{ShapeAdmission: tc.value}
			if got := limit.ResolvedShapeAdmission(); got != tc.want {
				t.Fatalf("resolved = %q, want %q", got, tc.want)
			}
			if got := limit.ShapeAdmissionEnabled(); got != tc.enabled {
				t.Fatalf("enabled = %v, want %v", got, tc.enabled)
			}
			if got := limit.ShapeAdmissionShadow(); got != tc.shadow {
				t.Fatalf("shadow = %v, want %v", got, tc.shadow)
			}
			if got := limit.ShapeAdmissionObserved(); got != tc.observed {
				t.Fatalf("observed = %v, want %v", got, tc.observed)
			}
		})
	}
	var nilLimit *NodeResourceLimit
	if nilLimit.ShapeAdmissionShadow() || !nilLimit.ShapeAdmissionObserved() {
		t.Fatal("nil limit must resolve to on and observe the shape rule")
	}
}

func TestAdmissionLayersValidateAcceptsShadow(t *testing.T) {
	str := func(v string) *string { return &v }
	for _, value := range []string{"on", "off", "shadow", " SHADOW "} {
		if err := (&NodeResourceLimit{ShapeAdmission: str(value)}).Validate(); err != nil {
			t.Fatalf("shape_admission %q rejected: %v", value, err)
		}
	}
	if err := (&NodeResourceLimit{ShapeAdmission: str("sometimes")}).Validate(); err == nil {
		t.Fatal("unknown shape_admission must be rejected at startup")
	}
}

// The versioned deployments must keep loading after the third mode exists.
func TestVersionedDeployConfigsLoadShapeAdmission(t *testing.T) {
	for _, path := range []string{
		filepath.Join("..", "..", "..", "deploy", "k8s", "base", "config", "scheduler.json"),
		filepath.Join("..", "..", "..", "deploy", "docker", "config", "default.json"),
	} {
		cfg, err := LoadScheduler(path, false)
		if err != nil {
			t.Fatalf("load %s failed: %v", path, err)
		}
		limit := cfg.Scheduler.NodeResourceLimit
		if limit == nil {
			t.Fatalf("%s must configure node_resource_limit", path)
		}
		if got := limit.ResolvedShapeAdmission(); got != ShapeAdmissionOn {
			t.Fatalf("%s shape_admission resolved to %q, want on", path, got)
		}
	}
}

// The Docker Compose deployment the e2e runs must publish the same admission
// verdict as the Kubernetes one. Without node_resource_limit the scheduler has
// no rule to evaluate and /nodes omits the admission object entirely.
func TestComposeDeployConfigPublishesAdmissionVerdicts(t *testing.T) {
	path := filepath.Join("..", "..", "..", "deploy", "docker", "config", "default.json")
	cfg, err := LoadScheduler(path, false)
	if err != nil {
		t.Fatalf("load compose config failed: %v", err)
	}
	limit := cfg.Scheduler.NodeResourceLimit
	if limit == nil {
		t.Fatal("compose config must configure node_resource_limit so /nodes publishes the verdict")
	}
	if !limit.ShapeAdmissionEnabled() {
		t.Fatal("compose config must keep shape admission on")
	}
	if got := limit.ResolvedMemoryCapacitySource(); got != MemoryCapacitySourceTotal {
		t.Fatalf("static discovery resolves capacity over memory_total, got %q", got)
	}
	layers := limit.ResolvedAdmissionLayers()
	want := []uint64{8, 16, 32, 64}
	if len(layers) != len(want) {
		t.Fatalf("got %d layers, want %d", len(layers), len(want))
	}
	for i, memoryGB := range want {
		if layers[i].MemoryGB != memoryGB {
			t.Fatalf("layer %d memory_gb = %d, want %d", i, layers[i].MemoryGB, memoryGB)
		}
	}
}

func TestResolvedAdmissionLayersIsACopy(t *testing.T) {
	limit := &NodeResourceLimit{AdmissionLayers: []AdmissionLayer{{MemoryGB: 12}}}
	layers := limit.ResolvedAdmissionLayers()
	layers[0].MemoryGB = 99
	if limit.AdmissionLayers[0].MemoryGB != 12 {
		t.Fatal("ResolvedAdmissionLayers must not alias the configured slice")
	}
}
