package connect

import (
	"context"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	signingName   = "connect"
	uaMarker      = "api/connect#"
	matchPriority = 87
)

// pathPrefixes are the first URI segments of every Connect operation; internal/routegates/path_gates.txt mirrors them.
var pathPrefixes = []string{ //nolint:gochecknoglobals // immutable route-gate list
	"/agent-status",
	"/analytics-data",
	"/associate-security-profiles",
	"/attached-files",
	"/auth",
	"/chat-integration-event",
	"/contact",
	"/cross-region-routing",
	"/data-tables",
	"/default-vocabulary",
	"/disassociate-security-profiles",
	"/effective-hours-of-operations",
	"/email-addresses",
	"/entity-security-profiles-summary",
	"/evaluation-forms",
	"/extraction-definitions",
	"/flow-associations",
	"/hours-of-operations",
	pathInstance,
	"/metrics",
	"/notifications",
	"/phone-number",
	"/predefined-attributes",
	"/prompts",
	"/push-notification",
	"/queues",
	"/quick-connects",
	"/routing-profiles",
	"/rules",
	"/search-",
	"/security-profiles",
	"/tags",
	"/test-case",
	"/traffic-distribution",
	"/user",
	"/views",
	"/vocabulary",
	"/workspaces",
}

// Handler serves the Amazon Connect REST-JSON API.
type Handler struct {
	Backend       *Backend
	server        *restroute.Server
	idem          *idempotency.Memo
	DefaultRegion string
}

// NewHandler wraps a backend.
func NewHandler(backend *Backend) *Handler {
	h := &Handler{Backend: backend, idem: idempotency.New(serviceName)}
	h.server = &restroute.Server{Router: restroute.New(routes()), Ops: h.ops()}

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "Connect" }

// Reset clears all state.
func (h *Handler) Reset() {
	h.Backend.Reset()
	h.idem.Clear()
}

// Snapshot implements persistence.Persistable.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// GetSupportedOperations lists every routed operation.
func (h *Handler) GetSupportedOperations() []string {
	ops := make([]string, 0, len(h.server.Ops))
	for op := range h.server.Ops {
		ops = append(ops, op)
	}

	slices.Sort(ops)

	return ops
}

// ChaosServiceName returns the lowercase service name for fault rules.
func (h *Handler) ChaosServiceName() string { return signingName }

// ChaosOperations lists the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions lists the regions served.
func (h *Handler) ChaosRegions() []string { return []string{h.DefaultRegion} }

// RouteMatcher claims requests signed for connect under a Connect URI prefix, so unrouted operations fail as unknown.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		path := restroute.EscapedPath(r)

		if httputils.ExtractServiceFromRequest(r) == signingName {
			return slices.ContainsFunc(pathPrefixes, func(p string) bool { return strings.HasPrefix(path, p) })
		}

		return service.MatchesUserAgentMarker(r.Header, uaMarker) && h.server.Router.Handles(path)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := h.server.Operation(c.Request())
	if op == "" {
		return "Unknown"
	}

	return op
}

// ExtractResource returns the instance ID for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, params := h.server.Operation(c.Request())

	return params["InstanceId"]
}

// Handler returns the Echo handler.
func (h *Handler) Handler() echo.HandlerFunc { return h.server.Handler() }

func replay[I, O any](
	h *Handler,
	op string,
	token func(*I) string,
	fn func(context.Context, *I) (*O, error),
) restroute.OpFunc {
	return restroute.Typed(func(ctx context.Context, in *I) (*O, error) {
		out, err := idempotency.Replay(
			h.idem,
			op,
			token(in),
			idempotency.Fingerprint(in),
			func() (*O, error) { return fn(ctx, in) },
		)

		return out, idempotencyErr(err)
	})
}
