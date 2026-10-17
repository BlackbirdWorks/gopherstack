package route53recoveryreadiness

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
	signingName   = "route53-recovery-readiness"
	uaMarker      = "api/route53recoveryreadiness"
	matchPriority = 87

	pathCell  = "/cells/{CellName}"
	pathGroup = "/recoverygroups/{RecoveryGroupName}"
	pathSet   = "/resourcesets/{ResourceSetName}"
	pathCheck = "/readinesschecks/{ReadinessCheckName}"
	pathTags  = "/tags/{ResourceArn}"
)

// Handler serves the Route 53 Recovery Readiness API.
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
func (h *Handler) Name() string { return "Route53RecoveryReadiness" }

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// Snapshot implements persistence.Persistable.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
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

// ExtractResource returns the first path label for metrics.
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
	g, p, d, u := http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodPut

	return []restroute.Route{
		{Method: p, Path: "/cells", Op: "CreateCell"},
		{Method: g, Path: "/cells", Op: "ListCells"},
		{Method: g, Path: pathCell, Op: "GetCell"},
		{Method: u, Path: pathCell, Op: "UpdateCell"},
		{Method: d, Path: pathCell, Op: "DeleteCell"},
		{Method: g, Path: "/cellreadiness/{CellName}", Op: "GetCellReadinessSummary"},
		{Method: p, Path: "/recoverygroups", Op: "CreateRecoveryGroup"},
		{Method: g, Path: "/recoverygroups", Op: "ListRecoveryGroups"},
		{Method: g, Path: pathGroup, Op: "GetRecoveryGroup"},
		{Method: u, Path: pathGroup, Op: "UpdateRecoveryGroup"},
		{Method: d, Path: pathGroup, Op: "DeleteRecoveryGroup"},
		{
			Method: g,
			Path:   "/recoverygroups/{RecoveryGroupName}/architectureRecommendations",
			Op:     "GetArchitectureRecommendations",
		},
		{Method: g, Path: "/recoverygroupreadiness/{RecoveryGroupName}", Op: "GetRecoveryGroupReadinessSummary"},
		{Method: p, Path: "/resourcesets", Op: "CreateResourceSet"},
		{Method: g, Path: "/resourcesets", Op: "ListResourceSets"},
		{Method: g, Path: pathSet, Op: "GetResourceSet"},
		{Method: u, Path: pathSet, Op: "UpdateResourceSet"},
		{Method: d, Path: pathSet, Op: "DeleteResourceSet"},
		{Method: p, Path: "/readinesschecks", Op: "CreateReadinessCheck"},
		{Method: g, Path: "/readinesschecks", Op: "ListReadinessChecks"},
		{Method: g, Path: pathCheck, Op: "GetReadinessCheck"},
		{Method: u, Path: pathCheck, Op: "UpdateReadinessCheck"},
		{Method: d, Path: pathCheck, Op: "DeleteReadinessCheck"},
		{Method: g, Path: "/readinesschecks/{ReadinessCheckName}/status", Op: "GetReadinessCheckStatus"},
		{
			Method: g, Path: "/readinesschecks/{ReadinessCheckName}/resource/{ResourceIdentifier}/status",
			Op: "GetReadinessCheckResourceStatus",
		},
		{Method: p, Path: "/crossaccountauthorizations", Op: "CreateCrossAccountAuthorization"},
		{Method: g, Path: "/crossaccountauthorizations", Op: "ListCrossAccountAuthorizations"},
		{
			Method: d,
			Path:   "/crossaccountauthorizations/{CrossAccountAuthorization}",
			Op:     "DeleteCrossAccountAuthorization",
		},
		{Method: g, Path: "/rules", Op: "ListRules"},
		{Method: g, Path: pathTags, Op: "ListTagsForResources"},
		{Method: p, Path: pathTags, Op: "TagResource"},
		{Method: d, Path: pathTags, Op: "UntagResource"},
	}
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	return map[string]restroute.OpFunc{
		"CreateCell":                       h.createCell,
		"GetCell":                          h.getCell,
		"ListCells":                        h.listCells,
		"UpdateCell":                       h.updateCell,
		"DeleteCell":                       h.deleteCell,
		"GetCellReadinessSummary":          h.cellSummary,
		"CreateRecoveryGroup":              h.createGroup,
		"GetRecoveryGroup":                 h.getGroup,
		"ListRecoveryGroups":               h.listGroups,
		"UpdateRecoveryGroup":              h.updateGroup,
		"DeleteRecoveryGroup":              h.deleteGroup,
		"GetArchitectureRecommendations":   h.recommendations,
		"GetRecoveryGroupReadinessSummary": h.groupSummary,
		"CreateResourceSet":                h.createSet,
		"GetResourceSet":                   h.getSet,
		"ListResourceSets":                 h.listSets,
		"UpdateResourceSet":                h.updateSet,
		"DeleteResourceSet":                h.deleteSet,
		"CreateReadinessCheck":             h.createCheck,
		"GetReadinessCheck":                h.getCheck,
		"ListReadinessChecks":              h.listChecks,
		"UpdateReadinessCheck":             h.updateCheck,
		"DeleteReadinessCheck":             h.deleteCheck,
		"GetReadinessCheckStatus":          h.checkStatus,
		"GetReadinessCheckResourceStatus":  h.resourceStatus,
		"CreateCrossAccountAuthorization":  h.createAuth,
		"ListCrossAccountAuthorizations":   h.listAuths,
		"DeleteCrossAccountAuthorization":  h.deleteAuth,
		"ListRules":                        h.listRules,
		"ListTagsForResources":             h.listTags,
		"TagResource":                      h.tagResource,
		"UntagResource":                    h.untagResource,
	}
}
