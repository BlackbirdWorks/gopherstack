package appintegrations

import (
	"context"
	"net/http"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	signingName   = "app-integrations"
	uaMarker      = "api/appintegrations"
	matchPriority = 87

	pathEventIntegrations = "/eventIntegrations"
	pathDataIntegrations  = "/dataIntegrations"
	pathApplications      = "/applications"
	pathTags              = "/tags/{resourceArn}"
)

// Handler serves the AppIntegrations REST-JSON API.
type Handler struct {
	Backend       *InMemoryBackend
	server        *restroute.Server
	idem          *idempotency.Memo
	DefaultRegion string
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend, idem: idempotency.New(serviceName)}
	h.server = &restroute.Server{Router: restroute.New(routes()), Ops: h.ops()}

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "AppIntegrations" }

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

// RouteMatcher matches by SigV4 signing name (or SDK user agent) plus a known path shape.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		if httputils.ExtractServiceFromRequest(r) != signingName &&
			!service.MatchesUserAgentMarker(r.Header, uaMarker) {
			return false
		}

		return h.server.Router.Handles(restroute.EscapedPath(r))
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
	const (
		get    = http.MethodGet
		post   = http.MethodPost
		del    = http.MethodDelete
		patch  = http.MethodPatch
		eiName = pathEventIntegrations + "/{Name}"
		diID   = pathDataIntegrations + "/{DataIntegrationIdentifier}"
		appArn = pathApplications + "/{Arn}"
	)

	return []restroute.Route{
		{Method: post, Path: pathEventIntegrations, Op: "CreateEventIntegration"},
		{Method: get, Path: pathEventIntegrations, Op: "ListEventIntegrations"},
		{Method: get, Path: eiName, Op: "GetEventIntegration"},
		{Method: patch, Path: eiName, Op: "UpdateEventIntegration"},
		{Method: del, Path: eiName, Op: "DeleteEventIntegration"},
		{
			Method: get,
			Path:   pathEventIntegrations + "/{EventIntegrationName}/associations",
			Op:     "ListEventIntegrationAssociations",
		},
		{Method: post, Path: pathDataIntegrations, Op: "CreateDataIntegration"},
		{Method: get, Path: pathDataIntegrations, Op: "ListDataIntegrations"},
		{Method: get, Path: pathDataIntegrations + "/{Identifier}", Op: "GetDataIntegration"},
		{Method: patch, Path: pathDataIntegrations + "/{Identifier}", Op: "UpdateDataIntegration"},
		{Method: del, Path: diID, Op: "DeleteDataIntegration"},
		{Method: post, Path: diID + "/associations", Op: "CreateDataIntegrationAssociation"},
		{Method: get, Path: diID + "/associations", Op: "ListDataIntegrationAssociations"},
		{
			Method: patch, Path: diID + "/associations/{DataIntegrationAssociationIdentifier}",
			Op: "UpdateDataIntegrationAssociation",
		},
		{Method: post, Path: pathApplications, Op: "CreateApplication"},
		{Method: get, Path: pathApplications, Op: "ListApplications"},
		{Method: get, Path: appArn, Op: "GetApplication"},
		{Method: patch, Path: appArn, Op: "UpdateApplication"},
		{Method: del, Path: appArn, Op: "DeleteApplication"},
		{Method: get, Path: pathApplications + "/{ApplicationId}/associations", Op: "ListApplicationAssociations"},
		{Method: get, Path: pathTags, Op: "ListTagsForResource"},
		{Method: post, Path: pathTags, Op: "TagResource"},
		{Method: del, Path: pathTags, Op: "UntagResource"},
	}
}
