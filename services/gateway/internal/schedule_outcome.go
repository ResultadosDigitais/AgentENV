package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"go.uber.org/zap"
)

// Settlement outcomes accepted by SettleAdmission. This is the closed set
// documented in scheduler.proto; the scheduler rejects anything else.
const (
	reservationOutcomeConfirmed = "confirmed"
	reservationOutcomeFailed    = "failed"
)

func (s *Server) recordAssignmentFromResponse(ctx context.Context, resp *http.Response, node *schedulerv1.Node, requestMethod, requestPath string, shape *sandboxShape) error {
	recordCtx, cancelRecord := context.WithTimeout(ctx, recordAssignmentTimeout(s.requestTimeout))
	defer cancelRecord()
	route, _ := matchPathRoute(requestMethod, requestPath)
	if route.owner == ownerNewBuild {
		if err := s.recordAssignment(recordCtx, route.id, node, "template_build"); err != nil {
			return &proxyResponseError{statusCode: http.StatusServiceUnavailable, message: "failed to route allocated build", cause: err}
		}
		return nil
	}
	if route.owner == ownerNewBuildConfirmed {
		buildID := strings.TrimSpace(resp.Header.Get("x-agentenv-build-id"))
		if buildID == "" {
			return &proxyResponseError{statusCode: http.StatusBadGateway, message: "upstream build response is missing its build ID"}
		}
		if buildID != route.id {
			return &proxyResponseError{statusCode: http.StatusBadGateway, message: "upstream build response ID does not match the request"}
		}
		if err := s.recordAssignment(recordCtx, buildID, node, "template_build"); err != nil {
			return &proxyResponseError{statusCode: http.StatusServiceUnavailable, message: "failed to route allocated build", cause: err}
		}
		return nil
	}

	if sandboxID, ok := sandboxIDFromHeaders(resp.Header); ok {
		if shape != nil {
			s.sandboxShapes.set(sandboxID, *shape)
		}
		s.recordAssignment(recordCtx, sandboxID, node, "response_header")
		return nil
	}

	body, truncated, err := readBodyWithLimit(resp.Body, s.maxRespSize)
	if err != nil {
		return &proxyResponseError{
			statusCode: http.StatusBadGateway,
			message:    "failed to read upstream response",
			cause:      err,
		}
	}
	if truncated {
		s.logger.Warn("upstream response exceeded configured forwarding limit",
			zap.Int64("max_response_size_bytes", s.maxRespSize),
			zap.Int64("upstream_content_length", resp.ContentLength),
			zap.String("content_type", resp.Header.Get("Content-Type")),
		)
		return &proxyResponseError{
			statusCode: http.StatusBadGateway,
			message:    "upstream response too large",
		}
	}
	_ = resp.Body.Close()

	resp.Body = io.NopCloser(bytes.NewReader(body))
	resp.ContentLength = int64(len(body))
	if resp.Header == nil {
		resp.Header = make(http.Header)
	}
	resp.Header.Set("Content-Length", strconv.Itoa(len(body)))

	ids := extractSandboxIDsFromResponse(body)
	if route.owner == ownerSandboxFork {
		ids = extractForkSandboxIDsFromResponse(body)
	}
	for _, sandboxID := range ids {
		if shape != nil {
			s.sandboxShapes.set(sandboxID, *shape)
		}
		s.recordAssignment(recordCtx, sandboxID, node, "response_body")
	}
	return nil
}

// writeCapacityDeferred answers a resume the scheduler deferred because the
// paused shape does not fit the owner. It is retryable: Retry-After tells the
// client when to try again and the body carries a stable code plus the closed
// admission reason (empty when the shape is not known yet).
func writeCapacityDeferred(w http.ResponseWriter, reason string) {
	w.Header().Set("Retry-After", "5")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusServiceUnavailable)
	body := struct {
		Code   string `json:"code"`
		Reason string `json:"reason,omitempty"`
	}{
		Code:   "sandbox_capacity_deferred",
		Reason: reason,
	}
	_ = json.NewEncoder(w).Encode(body)
}

func (s *Server) recordAssignment(ctx context.Context, sandboxID string, node *schedulerv1.Node, source string) error {
	rpcStart := time.Now()
	_, err := s.scheduler.RecordAssignment(ctx, &schedulerv1.RecordAssignmentRequest{SandboxId: sandboxID, Node: node})
	recordGatewaySchedulerRPC("RecordAssignment", rpcStart, err)
	if err != nil {
		s.logger.Warn("record assignment failed", zap.Error(err), zap.String("sandbox_id", sandboxID), zap.String("node_id", node.GetNodeId()))
		return err
	}

	s.logger.Debug("gateway recorded sandbox assignment",
		zap.String("sandbox_id", sandboxID),
		zap.String("node_id", node.GetNodeId()),
		zap.String("source", source),
	)
	return nil
}

func recordAssignmentTimeout(requestTimeout time.Duration) time.Duration {
	if requestTimeout <= 0 {
		return maxRecordAssignmentTimeout
	}
	if requestTimeout < maxRecordAssignmentTimeout {
		return requestTimeout
	}
	return maxRecordAssignmentTimeout
}

// settleAdmission releases the scheduler reservation an admitted create or
// resume created. It is best-effort: a failed settlement only means the
// reservation expires by its TTL, never that the proxied request fails. The
// call gets its own timeout detached from the request context, so a client
// that disconnects still lets the scheduler free the capacity.
func (s *Server) settleAdmission(ctx context.Context, reservationID string, outcome string) {
	if reservationID == "" {
		return
	}
	settleCtx, cancelSettle := context.WithTimeout(
		context.WithoutCancel(ctx),
		recordAssignmentTimeout(s.requestTimeout),
	)
	defer cancelSettle()

	rpcStart := time.Now()
	_, err := s.scheduler.SettleAdmission(settleCtx, &schedulerv1.SettleAdmissionRequest{
		ReservationId: reservationID,
		Outcome:       outcome,
	})
	recordGatewaySchedulerRPC("SettleAdmission", rpcStart, err)
	if err != nil {
		s.logger.Warn("settle admission reservation failed",
			zap.String("reservation_id", reservationID),
			zap.String("outcome", outcome),
			zap.Error(err),
		)
		return
	}
	s.logger.Debug("gateway settled admission reservation",
		zap.String("reservation_id", reservationID),
		zap.String("outcome", outcome),
	)
}

// reservationOutcomeForStatus maps the upstream status to the settlement
// outcome: only a 2xx means the sandbox was created or resumed.
func reservationOutcomeForStatus(statusCode int) string {
	if statusCode >= http.StatusOK && statusCode < http.StatusMultipleChoices {
		return reservationOutcomeConfirmed
	}
	return reservationOutcomeFailed
}
