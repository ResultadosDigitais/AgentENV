package scheduler

import (
	"testing"
	"time"
)

// fakeClock is a per-ledger clock so expiry tests never sleep and can run in
// parallel without sharing state.
type fakeClock struct {
	now time.Time
}

func (c *fakeClock) Now() time.Time { return c.now }

func (c *fakeClock) Advance(d time.Duration) { c.now = c.now.Add(d) }

func newTestLedger(ttl time.Duration) (*reservationLedger, *fakeClock) {
	clock := &fakeClock{now: time.Date(2026, 11, 5, 21, 35, 0, 0, time.UTC)}
	return newReservationLedger(ttl, clock.Now), clock
}

func TestReservationLedgerProjectsAdditiveShapes(t *testing.T) {
	ledger, _ := newTestLedger(time.Minute)

	ledger.Reserve("node-a", 30*gib, 4, 1, "")
	ledger.Reserve("node-a", 16*gib, 2, 1, "")
	ledger.Reserve("node-b", 64*gib, 8, 1, "")

	memoryBytes, cpuCount, slots, count := ledger.Project("node-a")
	if memoryBytes != 46*gib || cpuCount != 6 || slots != 2 || count != 2 {
		t.Fatalf("node-a projection = %d/%d/%d/%d, want 46GiB/6/2/2",
			memoryBytes, cpuCount, slots, count)
	}
	memoryBytes, cpuCount, _, count = ledger.Project("node-b")
	if memoryBytes != 64*gib || cpuCount != 8 || count != 1 {
		t.Fatalf("node-b projection = %d/%d/%d, want 64GiB/8/1", memoryBytes, cpuCount, count)
	}
	if memory, cpu, slots, count := ledger.Project("node-unknown"); memory != 0 || cpu != 0 || slots != 0 || count != 0 {
		t.Fatalf("unknown node projection = %d/%d/%d/%d, want zero", memory, cpu, slots, count)
	}
}

// A shape-less create still occupies one slot: the node admitted a sandbox
// even when its template shape is unknown.
func TestReservationLedgerZeroShapeCountsOneSlot(t *testing.T) {
	ledger, _ := newTestLedger(time.Minute)

	ledger.Reserve("node-a", 0, 0, 0, "")

	memoryBytes, cpuCount, slots, count := ledger.Project("node-a")
	if memoryBytes != 0 || cpuCount != 0 || slots != 1 || count != 1 {
		t.Fatalf("zero-shape projection = %d/%d/%d/%d, want 0/0/1/1",
			memoryBytes, cpuCount, slots, count)
	}
}

func TestReservationLedgerExpiresAfterTTL(t *testing.T) {
	ledger, clock := newTestLedger(60 * time.Second)
	ledger.Reserve("node-a", 30*gib, 4, 1, "")

	clock.Advance(59 * time.Second)
	if _, _, _, count := ledger.Project("node-a"); count != 1 {
		t.Fatalf("reservation at 59s = %d entries, want 1", count)
	}

	clock.Advance(time.Second)
	if memory, cpu, slots, count := ledger.Project("node-a"); memory != 0 || cpu != 0 || slots != 0 || count != 0 {
		t.Fatalf("expired reservation still projects: %d/%d/%d/%d", memory, cpu, slots, count)
	}
	// The pruned entry is gone from the map, not just hidden from the sum.
	ledger.mu.Lock()
	entries := len(ledger.entries)
	byKey := len(ledger.byKey)
	expired := ledger.expired
	ledger.mu.Unlock()
	if entries != 0 || byKey != 0 || expired != 1 {
		t.Fatalf("after expiry entries=%d byKey=%d expired=%d, want 0/0/1", entries, byKey, expired)
	}
}

// A failed outcome frees its reservation immediately and nothing else.
func TestReservationLedgerFailedSettlesOnlyItsOwnReservation(t *testing.T) {
	ledger, _ := newTestLedger(time.Minute)
	first := ledger.Reserve("node-a", 30*gib, 4, 1, "")
	ledger.Reserve("node-a", 30*gib, 4, 1, "")

	if !ledger.Settle(first, ReservationFailed) {
		t.Fatal("settling a live reservation must return true")
	}
	if _, _, _, count := ledger.Project("node-a"); count != 1 {
		t.Fatalf("after settling one, count = %d, want 1", count)
	}
	// A repeated settlement and an unknown id are no-ops.
	if ledger.Settle(first, ReservationFailed) {
		t.Fatal("a repeated settlement must return false")
	}
	if ledger.Settle("reservation-unknown", ReservationFailed) {
		t.Fatal("an unknown reservation must return false")
	}
	if _, _, _, count := ledger.Project("node-a"); count != 1 {
		t.Fatalf("a no-op settlement released another reservation: count = %d", count)
	}
}

// A confirmed reservation keeps projecting until the node reports a snapshot
// produced after the confirmation, which is the window between the upstream
// 2xx and the next heartbeat.
func TestReservationLedgerConfirmedHeldUntilObservation(t *testing.T) {
	ledger, clock := newTestLedger(time.Minute)
	id := ledger.Reserve("node-a", 30*gib, 4, 1, "")

	clock.Advance(2 * time.Second)
	if !ledger.Settle(id, ReservationConfirmed) {
		t.Fatal("confirming a live reservation must return true")
	}
	if memoryBytes, _, _, count := ledger.Project("node-a"); memoryBytes != 30*gib || count != 1 {
		t.Fatalf("a confirmed reservation must keep projecting until the next observation: %d bytes, %d entries", memoryBytes, count)
	}

	// A snapshot produced before the confirmation is still stale and must not
	// release the reservation.
	clock.Advance(-time.Second)
	if released := ledger.Observe("node-a", clock.Now()); released != 0 {
		t.Fatalf("a pre-confirmation snapshot released %d reservations", released)
	}
	clock.Advance(2 * time.Second)
	if released := ledger.Observe("node-b", clock.Now()); released != 0 {
		t.Fatalf("another node's snapshot released %d reservations", released)
	}
	if _, _, _, count := ledger.Project("node-a"); count != 1 {
		t.Fatalf("count = %d before the post-confirmation snapshot, want 1", count)
	}

	// The first snapshot produced after the confirmation releases it.
	if released := ledger.Observe("node-a", clock.Now()); released != 1 {
		t.Fatalf("post-confirmation snapshot released %d reservations, want 1", released)
	}
	if _, _, _, count := ledger.Project("node-a"); count != 0 {
		t.Fatalf("count = %d after the post-confirmation snapshot, want 0", count)
	}
}

// A repeated confirmation must not postpone the release forever.
func TestReservationLedgerRepeatedConfirmationIsNoop(t *testing.T) {
	ledger, clock := newTestLedger(time.Minute)
	id := ledger.Reserve("node-a", 30*gib, 4, 1, "")

	if !ledger.Settle(id, ReservationConfirmed) {
		t.Fatal("first confirmation must return true")
	}
	clock.Advance(10 * time.Second)
	if ledger.Settle(id, ReservationConfirmed) {
		t.Fatal("a repeated confirmation must return false")
	}
	if released := ledger.Observe("node-a", clock.Now()); released != 1 {
		t.Fatalf("the original confirmation time must still release the entry, released %d", released)
	}
}

// An expired reservation can no longer be settled, and settling it must not
// touch any live entry.
func TestReservationLedgerSettleExpiredIsNoop(t *testing.T) {
	ledger, clock := newTestLedger(30 * time.Second)
	expiredID := ledger.Reserve("node-a", 30*gib, 4, 1, "")
	clock.Advance(20 * time.Second)
	ledger.Reserve("node-a", 16*gib, 2, 1, "")

	// The first reservation is 31s old (expired); the second is 11s old.
	clock.Advance(11 * time.Second)
	if ledger.Settle(expiredID, ReservationConfirmed) {
		t.Fatal("settling an expired reservation must return false")
	}
	memoryBytes, _, _, count := ledger.Project("node-a")
	if memoryBytes != 16*gib || count != 1 {
		t.Fatalf("live reservation disturbed: %d bytes, %d entries", memoryBytes, count)
	}
}

// Reserving again with the sandbox key of an in-flight resume replaces the
// entry instead of doubling the footprint of the same sandbox.
func TestReservationLedgerKeyReplacesInsteadOfAdding(t *testing.T) {
	ledger, _ := newTestLedger(time.Minute)

	first := ledger.Reserve("owner", 16*gib, 4, 1, "sbx-1")
	second := ledger.Reserve("owner", 24*gib, 6, 1, "sbx-1")
	if first == second {
		t.Fatalf("a replacement must get a new opaque id, both were %q", first)
	}

	memoryBytes, cpuCount, slots, count := ledger.Project("owner")
	if memoryBytes != 24*gib || cpuCount != 6 || slots != 1 || count != 1 {
		t.Fatalf("replacement projection = %d/%d/%d/%d, want 24GiB/6/1/1",
			memoryBytes, cpuCount, slots, count)
	}
	if ledger.Settle(first, ReservationConfirmed) {
		t.Fatal("the replaced reservation must no longer be settleable")
	}
	if !ledger.Settle(second, ReservationConfirmed) {
		t.Fatal("the replacement must be settleable")
	}
}

// A repeated resume is decided without counting its own in-flight entry.
func TestReservationLedgerProjectExceptExcludesOwnKey(t *testing.T) {
	ledger, _ := newTestLedger(time.Minute)
	ledger.Reserve("owner", 16*gib, 4, 1, "sbx-1")
	ledger.Reserve("owner", 8*gib, 2, 1, "")

	memoryBytes, cpuCount, _, count := ledger.ProjectExcept("owner", "sbx-1")
	if memoryBytes != 8*gib || cpuCount != 2 || count != 1 {
		t.Fatalf("ProjectExcept = %d/%d/%d, want 8GiB/2/1", memoryBytes, cpuCount, count)
	}
	// Project still counts both.
	if _, _, _, count := ledger.Project("owner"); count != 2 {
		t.Fatalf("Project count = %d, want 2", count)
	}
}

// An empty key always creates a new entry: two creates of the same shape are
// two reservations, not one.
func TestReservationLedgerEmptyKeyAlwaysCreates(t *testing.T) {
	ledger, _ := newTestLedger(time.Minute)
	ledger.Reserve("node-a", 30*gib, 4, 1, "")
	ledger.Reserve("node-a", 30*gib, 4, 1, "")

	_, _, _, count := ledger.Project("node-a")
	if count != 2 {
		t.Fatalf("two keyless reservations = %d entries, want 2", count)
	}
}
