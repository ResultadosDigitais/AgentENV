package gateway

import "sync"

// sandboxShape is the template shape the gateway recorded when it scheduled a
// new sandbox. Until the node heartbeat reports per-sandbox paused resources,
// the resume path projects this shape so a resume goes through the same
// capacity rule as a new sandbox instead of failing open.
type sandboxShape struct {
	CPUCount    uint32
	MemoryBytes uint64
}

const maxSandboxShapes = 100_000

// sandboxShapeCache is an in-memory, per-gateway cache. The deployment runs a
// single gateway replica; a cache miss only makes the resume defer until the
// owner's heartbeat reports the paused shape, never over-admit.
type sandboxShapeCache struct {
	mu   sync.RWMutex
	byID map[string]sandboxShape
}

func newSandboxShapeCache() *sandboxShapeCache {
	return &sandboxShapeCache{byID: make(map[string]sandboxShape)}
}

func (c *sandboxShapeCache) set(sandboxID string, shape sandboxShape) {
	if sandboxID == "" || shape.MemoryBytes == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.byID) >= maxSandboxShapes {
		// Dropping the cache is safe: an absent shape defers the resume
		// instead of admitting it.
		c.byID = make(map[string]sandboxShape)
	}
	c.byID[sandboxID] = shape
}

func (c *sandboxShapeCache) get(sandboxID string) (sandboxShape, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	shape, ok := c.byID[sandboxID]
	return shape, ok
}

func (c *sandboxShapeCache) forget(sandboxID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.byID, sandboxID)
}
