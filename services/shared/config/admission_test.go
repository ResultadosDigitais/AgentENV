package config

import "testing"

func TestNodeResourceLimitAdmissionDefaults(t *testing.T) {
	var nilLimit *NodeResourceLimit
	if nilLimit.ResolvedAdmissionMode() != AdmissionModeNominal {
		t.Fatal("nil limit must resolve to nominal")
	}
	if err := nilLimit.Validate(); err != nil {
		t.Fatalf("nil limit must validate: %v", err)
	}
	limit := &NodeResourceLimit{}
	if limit.ResolvedAdmissionMode() != AdmissionModeNominal {
		t.Fatal("absent mode must resolve to nominal")
	}
	if limit.ResolvedMaxMemoryLimitOvercommitRatio() != 1.5 {
		t.Fatalf("default ratio must be 1.5, got %v", limit.ResolvedMaxMemoryLimitOvercommitRatio())
	}
}

func TestNodeResourceLimitValidateAdmission(t *testing.T) {
	str := func(v string) *string { return &v }
	f := func(v float64) *float64 { return &v }
	for _, mode := range []string{"nominal", "request", "shadow", " Request "} {
		if err := (&NodeResourceLimit{AdmissionMode: str(mode)}).Validate(); err != nil {
			t.Fatalf("mode %q should validate: %v", mode, err)
		}
	}
	if err := (&NodeResourceLimit{AdmissionMode: str("requests")}).Validate(); err == nil {
		t.Fatal("unknown mode must be rejected")
	}
	for _, r := range []float64{0, -1} {
		if err := (&NodeResourceLimit{MaxMemoryLimitOvercommitRatio: f(r)}).Validate(); err == nil {
			t.Fatalf("ratio %v must be rejected", r)
		}
	}
	if err := (&NodeResourceLimit{MaxMemoryLimitOvercommitRatio: f(2)}).Validate(); err != nil {
		t.Fatalf("ratio 2 should validate: %v", err)
	}
}
