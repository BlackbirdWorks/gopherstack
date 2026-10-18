// Package resourceexplorer2 emulates the AWS Resource Explorer control and search plane.
package resourceexplorer2

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
	uaMarker      = "api/resourceexplorer2"
	matchPriority = 87
	pathTags      = "/tags/{resourceArn}"
	tagRoutes     = 3
)

// Handler serves the Resource Explorer REST-JSON API.
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
func (h *Handler) Name() string { return "ResourceExplorer2" }

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
func (h *Handler) ChaosServiceName() string { return signingName }

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

// ExtractResource returns the ARN path label, when the operation has one.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, params := h.server.Operation(c.Request())

	return params["resourceArn"]
}

// Handler returns the echo handler.
func (h *Handler) Handler() echo.HandlerFunc { return h.server.Handler() }

func routes() []restroute.Route {
	posts := []string{
		"AssociateDefaultView", "BatchGetView", "CreateIndex", "CreateResourceExplorerSetup", "CreateView",
		"DeleteIndex", "DeleteResourceExplorerSetup", "DeleteView", "DisassociateDefaultView",
		"GetAccountLevelServiceConfiguration", "GetDefaultView", "GetIndex", "GetManagedView",
		"GetResourceExplorerSetup", "GetView", "ListIndexes", "ListIndexesForMembers", "ListManagedViews",
		"ListResources", "ListSupportedResourceTypes", "ListViews", "Search", "UpdateIndexType", "UpdateView",
		// Claimed so unimplemented operations answer UnknownOperationException instead of falling through to S3.
		"GetServiceIndex", "GetServiceView", "ListServiceIndexes", "ListServiceViews", "ListStreamingAccessForServices",
	}

	out := make([]restroute.Route, 0, len(posts)+tagRoutes)
	for _, op := range posts {
		out = append(out, restroute.Route{Method: http.MethodPost, Path: "/" + op, Op: op})
	}

	return append(out,
		restroute.Route{Method: http.MethodGet, Path: pathTags, Op: "ListTagsForResource"},
		restroute.Route{Method: http.MethodPost, Path: pathTags, Op: "TagResource"},
		restroute.Route{Method: http.MethodDelete, Path: pathTags, Op: "UntagResource"},
	)
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	return map[string]restroute.OpFunc{
		"AssociateDefaultView":                h.associateDefaultView,
		"BatchGetView":                        h.batchGetView,
		"CreateIndex":                         h.createIndex,
		"CreateResourceExplorerSetup":         h.createSetup,
		"CreateView":                          h.createView,
		"DeleteIndex":                         h.deleteIndex,
		"DeleteResourceExplorerSetup":         h.deleteSetup,
		"DeleteView":                          h.deleteView,
		"DisassociateDefaultView":             h.disassociateDefaultView,
		"GetAccountLevelServiceConfiguration": h.getAccountConfig,
		"GetDefaultView":                      h.getDefaultView,
		"GetIndex":                            h.getIndex,
		"GetManagedView":                      h.getManagedView,
		"GetResourceExplorerSetup":            h.getSetup,
		"GetView":                             h.getView,
		"ListIndexes":                         h.listIndexes,
		"ListIndexesForMembers":               h.listIndexesForMembers,
		"ListManagedViews":                    h.listManagedViews,
		"ListResources":                       h.listResources,
		"ListSupportedResourceTypes":          h.listSupportedTypes,
		"ListTagsForResource":                 h.listTags,
		"ListViews":                           h.listViews,
		"Search":                              h.search,
		"TagResource":                         h.tagResource,
		"UntagResource":                       h.untagResource,
		"UpdateIndexType":                     h.updateIndexType,
		"UpdateView":                          h.updateView,
	}
}
