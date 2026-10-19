// Package bedrockagentcorecontrol emulates the Amazon Bedrock AgentCore control plane.
package bedrockagentcorecontrol

import (
	"context"
	"net/http"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	uaMarker      = "api/bedrockagentcorecontrol"
	matchPriority = 87
)

// Handler serves the AgentCore control-plane REST-JSON API.
type Handler struct {
	Backend       *InMemoryBackend
	server        *restroute.Server
	DefaultRegion string
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend, DefaultRegion: backend.Region()}
	h.server = &restroute.Server{Router: restroute.New(routes()), Ops: h.ops(), Context: h.requestContext}

	return h
}

func (h *Handler) requestContext(r *http.Request) context.Context {
	ctx := r.Context()
	if _, ok := awsmeta.Key.Get(ctx); ok {
		return ctx
	}

	return awsmeta.Set(ctx, awsmeta.FromRequest(r, h.DefaultRegion))
}

// Name returns the service name.
func (h *Handler) Name() string { return "BedrockAgentCoreControl" }

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations lists every implemented operation.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.server.Ops))
	for op := range h.server.Ops {
		ops = append(ops, op)
	}

	slices.Sort(ops)

	return ops
}

// ChaosServiceName returns the lowercase service name for fault rules.
func (h *Handler) ChaosServiceName() string { return serviceName }

// ChaosOperations lists the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions lists the regions this handler serves.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher matches by SigV4 signing name (or SDK user agent) plus a known path shape.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if !h.server.Router.Handles(restroute.EscapedPath(r)) {
			return false
		}

		return httputils.ExtractServiceFromRequest(r) == signingName ||
			service.MatchesUserAgentMarker(r.Header, uaMarker)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation returns the operation name.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := h.server.Operation(c.Request())
	if op == "" {
		return "Unknown"
	}

	return op
}

// ExtractResource returns the resource ARN or identifier the request addresses.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, params := h.server.Operation(c.Request())

	keys := []string{"resourceArn", keyAgentRuntimeID, keyGatewayIdentifier, "memoryId", keyHarnessID, keyDatasetID}

	for _, k := range keys {
		if v := params[k]; v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the echo handler.
func (h *Handler) Handler() echo.HandlerFunc { return h.server.Handler() }
