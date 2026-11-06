package scheduler

import (
	"strconv"
	"sync"
	"time"

	"agentenv/services/shared/config"
)

// Admission reservation outcomes accepted by the gateway's SettleAdmission
// RPC. This is a closed set: a settlement that carries anything else is
// rejected at the RPC boundary.
const (
	ReservationConfirmed = "confirmed"
	ReservationFailed    = "failed"
)

// admissionReservation is one admitted decision the scheduler projects onto a
// node until the node's observed state accounts for it or the TTL elapses: a
// create that is still starting or a resume that is still bringing its VM
// back. A failed outcome releases it immediately; a confirmed outcome keeps it
// until the node reports a snapshot produced after the confirmation, because
// the admission decision reads the heartbeat snapshot and that snapshot does
// not yet include the new sandbox at the moment the gateway sees the 2xx.
type admissionReservation struct {
	id          string
	nodeID      string
	memoryBytes uint64
	cpuCount    uint32
	slots       uint32
	key         string
	createdAt   time.Time
	expiresAt   time.Time
	// confirmedAt is zero while the gateway has not answered. Once the gateway
	// confirms the create or resume it holds the time of that confirmation;
	// the reservation stops projecting on the first snapshot observed after it.
	confirmedAt time.Time
}

// reservationLedger holds the in-flight reservations of every node. It has
// its own mutex; the scheduler serializes the whole read-decide-reserve
// sequence with admissionMu and takes this lock inside it. The clock is
// injectable per ledger so expiry tests never sleep and never share state.
type reservationLedger struct {
	mu      sync.Mutex
	now     func() time.Time
	ttl     time.Duration
	seq     uint64
	entries map[string]admissionReservation
	// byKey indexes the idempotent reservations (a resume keyed by sandbox
	// ID) so a repeated decision replaces its own entry instead of adding a
	// second one.
	byKey   map[string]string
	expired uint64
}

func newReservationLedger(ttl time.Duration, now func() time.Time) *reservationLedger {
	if now == nil {
		now = time.Now
	}
	if ttl <= 0 {
		ttl = config.DefaultAdmissionReservationTTL
	}
	return &reservationLedger{
		now:     now,
		ttl:     ttl,
		entries: make(map[string]admissionReservation),
		byKey:   make(map[string]string),
	}
}

// Reserve records one admitted shape and returns its opaque id. A non-empty
// key replaces the previous reservation carrying the same key (the sandbox ID
// of a resume), so repeating a resume never doubles its footprint. An empty
// key always creates a new entry (a create). A zero shape still occupies one
// slot: the node admitted a sandbox even if its template shape is unknown.
func (l *reservationLedger) Reserve(nodeID string, memoryBytes uint64, cpuCount uint32, slots uint32, key string) string {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	l.pruneLocked(now)
	if key != "" {
		l.removeKeyLocked(key)
	}
	if slots == 0 {
		slots = 1
	}
	l.seq++
	id := "reservation-" + strconv.FormatUint(l.seq, 10)
	l.entries[id] = admissionReservation{
		id:          id,
		nodeID:      nodeID,
		memoryBytes: memoryBytes,
		cpuCount:    cpuCount,
		slots:       slots,
		key:         key,
		createdAt:   now,
		expiresAt:   now.Add(l.ttl),
	}
	if key != "" {
		l.byKey[key] = id
	}
	recordAdmissionReservation("reserved")
	return id
}

// Settle applies the gateway's outcome to one reservation. A failed create or
// resume frees its capacity immediately. A confirmed one keeps projecting
// until Observe sees a snapshot of the node produced after this confirmation,
// so the window between the upstream 2xx and the next heartbeat cannot admit a
// second create into the space the first one just took.
//
// An unknown, repeated or already expired id is a no-op that returns false:
// settlement is best-effort and idempotent, so a gateway retrying after a
// network error cannot release another create's reservation, and a repeated
// confirmation cannot keep postponing the release of its own entry.
func (l *reservationLedger) Settle(id string, outcome string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.pruneLocked(l.now())
	entry, ok := l.entries[id]
	if !ok {
		return false
	}
	if outcome == ReservationConfirmed {
		if !entry.confirmedAt.IsZero() {
			return false
		}
		entry.confirmedAt = l.now()
		l.entries[id] = entry
		recordAdmissionReservation("confirmed")
		return true
	}
	if outcome != ReservationFailed {
		return false
	}
	delete(l.entries, id)
	if entry.key != "" && l.byKey[entry.key] == id {
		delete(l.byKey, entry.key)
	}
	recordAdmissionReservation("settled")
	return true
}

// Observe releases every confirmed reservation of one node whose snapshot was
// produced after the confirmation. It returns how many entries it released.
// In-flight (not yet settled) reservations are never touched here: only the
// gateway's outcome or the TTL can end those.
func (l *reservationLedger) Observe(nodeID string, observedAt time.Time) int {
	if observedAt.IsZero() {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	l.pruneLocked(l.now())
	released := 0
	for id, entry := range l.entries {
		if entry.nodeID != nodeID || entry.confirmedAt.IsZero() {
			continue
		}
		if !observedAt.After(entry.confirmedAt) {
			continue
		}
		delete(l.entries, id)
		if entry.key != "" && l.byKey[entry.key] == id {
			delete(l.byKey, entry.key)
		}
		released++
	}
	if released > 0 {
		recordAdmissionReservationCount("released", released)
	}
	return released
}

// Project returns the reserved totals of one node and how many reservations
// they come from. Expired reservations are pruned before the sum.
func (l *reservationLedger) Project(nodeID string) (memoryBytes uint64, cpuCount uint32, slots uint32, count int) {
	return l.ProjectExcept(nodeID, "")
}

// ProjectExcept is Project without the reservation carrying excludeKey. A
// repeated resume of the same sandbox is decided without counting its own
// in-flight reservation, which Reserve then replaces under the same lock.
func (l *reservationLedger) ProjectExcept(nodeID string, excludeKey string) (memoryBytes uint64, cpuCount uint32, slots uint32, count int) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.pruneLocked(l.now())
	for _, entry := range l.entries {
		if entry.nodeID != nodeID {
			continue
		}
		if excludeKey != "" && entry.key == excludeKey {
			continue
		}
		memoryBytes += entry.memoryBytes
		cpuCount += entry.cpuCount
		slots += entry.slots
		count++
	}
	return memoryBytes, cpuCount, slots, count
}

// pruneLocked drops every reservation whose TTL elapsed. l.mu must be held.
func (l *reservationLedger) pruneLocked(now time.Time) int {
	removed := 0
	for id, entry := range l.entries {
		if now.Before(entry.expiresAt) {
			continue
		}
		delete(l.entries, id)
		if entry.key != "" && l.byKey[entry.key] == id {
			delete(l.byKey, entry.key)
		}
		removed++
	}
	if removed > 0 {
		l.expired += uint64(removed)
		recordAdmissionReservationCount("expired", removed)
	}
	return removed
}

// removeKeyLocked drops the reservation indexed by key, if any. l.mu must be
// held.
func (l *reservationLedger) removeKeyLocked(key string) {
	id, ok := l.byKey[key]
	if !ok {
		return
	}
	delete(l.byKey, key)
	delete(l.entries, id)
}
