package gateway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

const (
	// templateShapeCacheTTL bounds how long a resolved template shape is
	// trusted. A rebuild that repoints an alias changes the shape, so a short
	// TTL keeps the cache fresh; a stale entry only affects admission, never
	// the forwarded create body.
	templateShapeCacheTTL = 5 * time.Minute
	// templateShapeLookupTimeout caps the internal template read. The measured
	// read takes 30-53 ms, so one second is a generous ceiling.
	templateShapeLookupTimeout = time.Second

	templateShapeOutcomeResolved     = "resolved"
	templateShapeOutcomeCacheHit     = "cache_hit"
	templateShapeOutcomeNotFound     = "not_found"
	templateShapeOutcomeLookupFailed = "lookup_failed"
	templateShapeOutcomeUnavailable  = "unavailable"
	templateShapeOutcomeNoNode       = "no_node"
)

// templateShapeCache is an in-memory, per-gateway cache keyed by the exact
// templateID the client sent. Only resolved shapes are cached; a miss or an
// expired entry falls through to a network read.
type templateShapeCache struct {
	mu         sync.Mutex
	byTemplate map[string]templateShapeCacheEntry
	now        func() time.Time
}

type templateShapeCacheEntry struct {
	shape     sandboxShape
	expiresAt time.Time
}

func newTemplateShapeCache(now func() time.Time) *templateShapeCache {
	if now == nil {
		now = time.Now
	}
	return &templateShapeCache{byTemplate: make(map[string]templateShapeCacheEntry), now: now}
}

func (c *templateShapeCache) get(templateID string) (sandboxShape, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.byTemplate[templateID]
	if !ok {
		return sandboxShape{}, false
	}
	if c.now().After(entry.expiresAt) {
		delete(c.byTemplate, templateID)
		return sandboxShape{}, false
	}
	return entry.shape, true
}

func (c *templateShapeCache) set(templateID string, shape sandboxShape) {
	if templateID == "" || shape.MemoryBytes == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.byTemplate[templateID] = templateShapeCacheEntry{
		shape:     shape,
		expiresAt: c.now().Add(templateShapeCacheTTL),
	}
}

func recordTemplateShapeLookup(outcome string, elapsed time.Duration) {
	gatewayTemplateShapeTotal.WithLabelValues(outcome).Inc()
	gatewayTemplateShapeLookupSeconds.WithLabelValues(outcome).Observe(elapsed.Seconds())
}

// templateBuild is the subset of GET /templates/{id} the resolver reads.
type templateBuild struct {
	CPUCount uint32 `json:"cpuCount"`
	MemoryMB uint64 `json:"memoryMB"`
}

type templateDetail struct {
	Builds []templateBuild `json:"builds"`
}

// resolveTemplateShape reads the shape of a template from a READY node before
// the create is scheduled. It never fails the create: every lookup failure is
// reported to the scheduler as a typed status and the caller keeps going.
func (s *Server) resolveTemplateShape(ctx context.Context, templateID string) (sandboxShape, schedulerv1.TemplateShapeStatus) {
	if shape, ok := s.templateShapes.get(templateID); ok {
		recordTemplateShapeLookup(templateShapeOutcomeCacheHit, 0)
		return shape, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_RESOLVED
	}

	start := time.Now()
	endpoint, ok := s.templateLookupEndpoint(ctx)
	if !ok {
		recordTemplateShapeLookup(templateShapeOutcomeNoNode, time.Since(start))
		return sandboxShape{}, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_UNAVAILABLE
	}

	lookupCtx, cancel := context.WithTimeout(ctx, templateShapeLookupTimeout)
	defer cancel()
	url := strings.TrimRight(endpoint, "/") + "/templates/" + templateID
	req, err := http.NewRequestWithContext(lookupCtx, http.MethodGet, url, nil)
	if err != nil {
		recordTemplateShapeLookup(templateShapeOutcomeLookupFailed, time.Since(start))
		return sandboxShape{}, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED
	}
	req.Header.Set(headerAPIKey, string(s.apiKey))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		recordTemplateShapeLookup(templateShapeOutcomeLookupFailed, time.Since(start))
		return sandboxShape{}, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusNotFound:
		recordTemplateShapeLookup(templateShapeOutcomeNotFound, time.Since(start))
		return sandboxShape{}, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_NOT_FOUND
	case resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices:
		recordTemplateShapeLookup(templateShapeOutcomeLookupFailed, time.Since(start))
		return sandboxShape{}, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED
	}

	var detail templateDetail
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&detail); err != nil {
		recordTemplateShapeLookup(templateShapeOutcomeLookupFailed, time.Since(start))
		return sandboxShape{}, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_LOOKUP_FAILED
	}
	if len(detail.Builds) == 0 || detail.Builds[0].MemoryMB == 0 {
		recordTemplateShapeLookup(templateShapeOutcomeUnavailable, time.Since(start))
		return sandboxShape{}, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_UNAVAILABLE
	}

	shape := sandboxShape{
		CPUCount:    detail.Builds[0].CPUCount,
		MemoryBytes: detail.Builds[0].MemoryMB << 20,
	}
	s.templateShapes.set(templateID, shape)
	recordTemplateShapeLookup(templateShapeOutcomeResolved, time.Since(start))
	s.logger.Debug("gateway resolved template shape",
		zap.String("template_id", templateID),
		zap.String("endpoint", endpoint),
		zap.Uint64("memory_bytes", shape.MemoryBytes),
		zap.Uint32("cpu_count", shape.CPUCount),
	)
	return shape, schedulerv1.TemplateShapeStatus_TEMPLATE_SHAPE_STATUS_RESOLVED
}

// templateLookupEndpoint picks a READY node with an endpoint from the
// scheduler's observed node list.
func (s *Server) templateLookupEndpoint(ctx context.Context) (string, bool) {
	rpcStart := time.Now()
	resp, err := s.scheduler.ListObservedNodes(ctx, &schedulerv1.ListObservedNodesRequest{})
	recordGatewaySchedulerRPC("ListObservedNodes", rpcStart, err)
	if err != nil {
		return "", false
	}
	for _, node := range resp.GetNodes() {
		if node.GetSnapshot().GetStatus() != schedulerv1.NodeStatus_NODE_STATUS_READY {
			continue
		}
		if endpoint := strings.TrimSpace(node.GetEndpoint()); endpoint != "" {
			return endpoint, true
		}
	}
	return "", false
}
