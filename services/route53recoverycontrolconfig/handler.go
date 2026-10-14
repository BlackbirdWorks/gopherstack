package route53recoverycontrolconfig

import (
	"context"
	"net/http"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	signingName   = "route53-recovery-control-config"
	pathCluster   = "/cluster"
	pathTags      = "/tags/{ResourceArn}"
	uaMarker      = "api/route53recoverycontrolconfig"
	matchPriority = 87
)

// Handler serves the Route 53 Recovery Control Config control plane.
type Handler struct {
	Backend       *InMemoryBackend
	server        *restroute.Server
	DefaultRegion string
}

// NewHandler creates the handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.server = &restroute.Server{Router: restroute.New(routes()), Ops: h.ops()}

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "Route53RecoveryControlConfig" }

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Snapshot implements persistence.Persistable.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}

// RoutingControlIsOn reports (on, found) for a routing control, for sibling services.
func (h *Handler) RoutingControlIsOn(arn string) (bool, bool) {
	return h.Backend.RoutingControlIsOn(arn)
}

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
func (h *Handler) ChaosServiceName() string { return signingName }

// ChaosOperations returns the fault-injectable operations.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns the regions served.
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

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _ := h.server.Operation(c.Request())
	if op == "" {
		return "Unknown"
	}

	return op
}

// ExtractResource returns the primary ARN label for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, params := h.server.Operation(c.Request())
	for _, v := range params {
		return v
	}

	return ""
}

// Handler returns the Echo handler.
func (h *Handler) Handler() echo.HandlerFunc { return h.server.Handler() }

func routes() []restroute.Route {
	return []restroute.Route{
		{Method: http.MethodPost, Path: pathCluster, Op: "CreateCluster"},
		{Method: http.MethodGet, Path: pathCluster, Op: "ListClusters"},
		{Method: http.MethodPut, Path: pathCluster, Op: "UpdateCluster"},
		{Method: http.MethodGet, Path: "/cluster/{ClusterArn}", Op: "DescribeCluster"},
		{Method: http.MethodDelete, Path: "/cluster/{ClusterArn}", Op: "DeleteCluster"},
		{Method: http.MethodPost, Path: "/controlpanel", Op: "CreateControlPanel"},
		{Method: http.MethodPut, Path: "/controlpanel", Op: "UpdateControlPanel"},
		{Method: http.MethodGet, Path: "/controlpanels", Op: "ListControlPanels"},
		{Method: http.MethodGet, Path: "/controlpanel/{ControlPanelArn}", Op: "DescribeControlPanel"},
		{Method: http.MethodDelete, Path: "/controlpanel/{ControlPanelArn}", Op: "DeleteControlPanel"},
		{Method: http.MethodGet, Path: "/controlpanel/{ControlPanelArn}/routingcontrols", Op: "ListRoutingControls"},
		{Method: http.MethodGet, Path: "/controlpanel/{ControlPanelArn}/safetyrules", Op: "ListSafetyRules"},
		{Method: http.MethodPost, Path: "/routingcontrol", Op: "CreateRoutingControl"},
		{Method: http.MethodPut, Path: "/routingcontrol", Op: "UpdateRoutingControl"},
		{Method: http.MethodGet, Path: "/routingcontrol/{RoutingControlArn}", Op: "DescribeRoutingControl"},
		{Method: http.MethodDelete, Path: "/routingcontrol/{RoutingControlArn}", Op: "DeleteRoutingControl"},
		{
			Method: http.MethodGet, Path: "/routingcontrol/{RoutingControlArn}/associatedRoute53HealthChecks",
			Op: "ListAssociatedRoute53HealthChecks",
		},
		{Method: http.MethodPost, Path: "/safetyrule", Op: "CreateSafetyRule"},
		{Method: http.MethodPut, Path: "/safetyrule", Op: "UpdateSafetyRule"},
		{Method: http.MethodGet, Path: "/safetyrule/{SafetyRuleArn}", Op: "DescribeSafetyRule"},
		{Method: http.MethodDelete, Path: "/safetyrule/{SafetyRuleArn}", Op: "DeleteSafetyRule"},
		{Method: http.MethodGet, Path: "/resourcePolicy/{ResourceArn}", Op: "GetResourcePolicy"},
		{Method: http.MethodGet, Path: pathTags, Op: "ListTagsForResource"},
		{Method: http.MethodPost, Path: pathTags, Op: "TagResource"},
		{Method: http.MethodDelete, Path: pathTags, Op: "UntagResource"},
	}
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	return map[string]restroute.OpFunc{
		"CreateCluster":                     h.createCluster,
		"DescribeCluster":                   h.describeCluster,
		"ListClusters":                      h.listClusters,
		"UpdateCluster":                     h.updateCluster,
		"DeleteCluster":                     h.deleteCluster,
		"CreateControlPanel":                h.createControlPanel,
		"DescribeControlPanel":              h.describeControlPanel,
		"ListControlPanels":                 h.listControlPanels,
		"UpdateControlPanel":                h.updateControlPanel,
		"DeleteControlPanel":                h.deleteControlPanel,
		"CreateRoutingControl":              h.createRoutingControl,
		"DescribeRoutingControl":            h.describeRoutingControl,
		"ListRoutingControls":               h.listRoutingControls,
		"UpdateRoutingControl":              h.updateRoutingControl,
		"DeleteRoutingControl":              h.deleteRoutingControl,
		"ListAssociatedRoute53HealthChecks": h.listAssociatedHealthChecks,
		"CreateSafetyRule":                  h.createSafetyRule,
		"DescribeSafetyRule":                h.describeSafetyRule,
		"ListSafetyRules":                   h.listSafetyRules,
		"UpdateSafetyRule":                  h.updateSafetyRule,
		"DeleteSafetyRule":                  h.deleteSafetyRule,
		"GetResourcePolicy":                 h.getResourcePolicy,
		"ListTagsForResource":               h.listTags,
		"TagResource":                       h.tagResource,
		"UntagResource":                     h.untagResource,
	}
}
