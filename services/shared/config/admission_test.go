package config

import (
	"encoding/json"
	"testing"
	"time"
)

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

func TestNodeResourceLimitAdmissionReservationDefaults(t *testing.T) {
	var nilLimit *NodeResourceLimit
	if !nilLimit.AdmissionReservationEnabled() {
		t.Fatal("nil limit must keep the reservation on")
	}
	if nilLimit.ResolvedAdmissionReservationTTL() != DefaultAdmissionReservationTTL {
		t.Fatal("nil limit must resolve the default TTL")
	}
	limit := &NodeResourceLimit{}
	if limit.ResolvedAdmissionReservation() != ShapeAdmissionOn || !limit.AdmissionReservationEnabled() {
		t.Fatal("absent admission_reservation must resolve to on")
	}
	if got := limit.ResolvedAdmissionReservationTTL(); got != DefaultAdmissionReservationTTL {
		t.Fatalf("absent admission_reservation_ttl = %v, want %v", got, DefaultAdmissionReservationTTL)
	}
	if DefaultAdmissionReservationTTL != DefaultGatewayRequestTimeout+AdmissionReservationTTLMargin {
		t.Fatalf("default ttl %v is not the default request timeout %v plus the margin %v",
			DefaultAdmissionReservationTTL, DefaultGatewayRequestTimeout, AdmissionReservationTTLMargin)
	}
}

func TestNodeResourceLimitAdmissionReservationParsesJSON(t *testing.T) {
	var limit NodeResourceLimit
	raw := `{"admission_reservation": " OFF ", "admission_reservation_ttl": "45s"}`
	if err := json.Unmarshal([]byte(raw), &limit); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if err := limit.Validate(); err != nil {
		t.Fatalf("valid keys rejected: %v", err)
	}
	if limit.AdmissionReservationEnabled() {
		t.Fatal(`"off" must disable the reservation`)
	}
	if got := limit.ResolvedAdmissionReservationTTL(); got != 45*time.Second {
		t.Fatalf("ttl = %v, want 45s", got)
	}
}

func TestNodeResourceLimitValidateAdmissionReservation(t *testing.T) {
	str := func(v string) *string { return &v }
	cases := []struct {
		name  string
		limit NodeResourceLimit
		ok    bool
	}{
		{"on", NodeResourceLimit{AdmissionReservation: str("on")}, true},
		{"off", NodeResourceLimit{AdmissionReservation: str("off")}, true},
		{"unknown switch", NodeResourceLimit{AdmissionReservation: str("enabled")}, false},
		{"duration", NodeResourceLimit{AdmissionReservationTTL: str("2m")}, true},
		{"not a duration", NodeResourceLimit{AdmissionReservationTTL: str("60")}, false},
		{"zero", NodeResourceLimit{AdmissionReservationTTL: str("0s")}, false},
		{"negative", NodeResourceLimit{AdmissionReservationTTL: str("-5s")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.limit.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("Validate() = %v, want ok=%v", err, tc.ok)
			}
			if !tc.ok && tc.limit.AdmissionReservationTTL != nil {
				// An invalid TTL never reaches the ledger: it resolves to the default.
				if got := tc.limit.ResolvedAdmissionReservationTTL(); got != DefaultAdmissionReservationTTL {
					t.Fatalf("invalid ttl resolved to %v", got)
				}
			}
		})
	}
}
