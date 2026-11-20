package admission_test

import (
	"encoding/json"
	"os"
	"testing"

	"agentenv/services/shared/admission"
	"agentenv/services/shared/config"
)

const vectorsPath = "testdata/agentenv-admission-ceiling-vectors.json"

// ceilingVector is one row of the shared fixture. The same fixture is loaded by
// the scheduler and the gateway tests, so the three call sites cannot drift.
type ceilingVector struct {
	Name                   string  `json:"name"`
	Detail                 string  `json:"detail"`
	MemoryTotalBytes       uint64  `json:"memoryTotalBytes"`
	CPUCount               uint32  `json:"cpuCount"`
	MarginBytes            *uint64 `json:"marginBytes"`
	MarginAbsent           bool    `json:"marginAbsent"`
	ApplyLegacyCaps        bool    `json:"applyLegacyCaps"`
	WantReason             string  `json:"wantReason"`
	WantApplied            *bool   `json:"wantApplied"`
	WantMemoryCeilingBytes *uint64 `json:"wantMemoryCeilingBytes"`
	WantCPUCeilingCores    *uint32 `json:"wantCpuCeilingCores"`
}

type ceilingFixtures struct {
	MarginBytes            uint64          `json:"marginBytes"`
	MaxCPUAllocatedPercent uint32          `json:"maxCpuAllocatedPercent"`
	LegacyMemoryCapBytes   uint64          `json:"legacyMemoryCapBytes"`
	LegacyCPUCapCores      uint32          `json:"legacyCpuCapCores"`
	Vectors                []ceilingVector `json:"vectors"`
}

func loadCeilingFixtures(t *testing.T) ceilingFixtures {
	t.Helper()
	data, err := os.ReadFile(vectorsPath)
	if err != nil {
		t.Fatalf("read %s: %v", vectorsPath, err)
	}
	var fixtures ceilingFixtures
	if err := json.Unmarshal(data, &fixtures); err != nil {
		t.Fatalf("decode %s: %v", vectorsPath, err)
	}
	if len(fixtures.Vectors) == 0 {
		t.Fatalf("%s carries no vectors", vectorsPath)
	}
	return fixtures
}

func limitFor(fixtures ceilingFixtures, vector ceilingVector) *config.NodeResourceLimit {
	limit := &config.NodeResourceLimit{MaxCPUAllocatedPercent: uint32Ptr(fixtures.MaxCPUAllocatedPercent)}
	if !vector.MarginAbsent {
		margin := fixtures.MarginBytes
		if vector.MarginBytes != nil {
			margin = *vector.MarginBytes
		}
		limit.MaxAllocatedMemoryMarginBytes = &margin
	}
	if vector.ApplyLegacyCaps {
		memoryCap := fixtures.LegacyMemoryCapBytes
		cpuCap := fixtures.LegacyCPUCapCores
		limit.MaxAllocatedMemoryBytesIncludingPaused = &memoryCap
		limit.MaxAllocatedCPUIncludingPaused = &cpuCap
	}
	return limit
}

func uint32Ptr(v uint32) *uint32 { return &v }

// IncludingPausedCeilingsOf is the single definition of the effective
// including-paused ceilings; every vector of the p-1 fixture must reproduce
// byte for byte, including the negative control for accidental activation.
func TestIncludingPausedCeilingsVectors(t *testing.T) {
	fixtures := loadCeilingFixtures(t)
	for _, vector := range fixtures.Vectors {
		t.Run(vector.Name, func(t *testing.T) {
			ceilings, reason := admission.IncludingPausedCeilingsOf(
				limitFor(fixtures, vector), vector.MemoryTotalBytes, vector.CPUCount)

			if vector.WantReason != "" {
				if reason != vector.WantReason {
					t.Fatalf("reason = %q, want %q (%s)", reason, vector.WantReason, vector.Detail)
				}
				if vector.WantApplied != nil && ceilings.Applied != *vector.WantApplied {
					t.Fatalf("applied = %v, want %v", ceilings.Applied, *vector.WantApplied)
				}
				return
			}
			if reason != "" {
				t.Fatalf("unexpected reason %q (%s)", reason, vector.Detail)
			}
			if vector.WantApplied != nil && ceilings.Applied != *vector.WantApplied {
				t.Fatalf("applied = %v, want %v", ceilings.Applied, *vector.WantApplied)
			}
			if vector.WantMemoryCeilingBytes != nil {
				if !ceilings.MemoryKnown || ceilings.MemoryBytes != *vector.WantMemoryCeilingBytes {
					t.Fatalf("memory ceiling = %d (known=%v), want %d (%s)",
						ceilings.MemoryBytes, ceilings.MemoryKnown, *vector.WantMemoryCeilingBytes, vector.Detail)
				}
			}
			if vector.WantCPUCeilingCores != nil {
				if !ceilings.CPUKnown || ceilings.CPUCores != *vector.WantCPUCeilingCores {
					t.Fatalf("cpu ceiling = %d (known=%v), want %d (%s)",
						ceilings.CPUCores, ceilings.CPUKnown, *vector.WantCPUCeilingCores, vector.Detail)
				}
			}
		})
	}
}

// The margin key decodes from the scheduler config JSON; the contract marker
// and the named reason are the strings the ProductFactory ceiling reader
// recognises.
func TestMarginKeyDecodesAndContractIsRecognised(t *testing.T) {
	var limit config.NodeResourceLimit
	if err := json.Unmarshal([]byte(`{"max_allocated_memory_margin_bytes": 8589934592}`), &limit); err != nil {
		t.Fatalf("decode margin key: %v", err)
	}
	if limit.MaxAllocatedMemoryMarginBytes == nil || *limit.MaxAllocatedMemoryMarginBytes != 8589934592 {
		t.Fatalf("margin key did not decode: %+v", limit.MaxAllocatedMemoryMarginBytes)
	}
	if admission.ContractPerNodeCeilingV1 != "per_node_ceiling_v1" {
		t.Fatalf("contract marker = %q", admission.ContractPerNodeCeilingV1)
	}
	if admission.ReasonCapacityUnknown != "capacity_unknown" {
		t.Fatalf("capacity reason = %q", admission.ReasonCapacityUnknown)
	}
}
