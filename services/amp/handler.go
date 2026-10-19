package amp

import (
	"context"
	"net/http"
	"net/url"
	"slices"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awsmeta"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	signingName   = "aps"
	uaMarker      = "api/amp#"
	matchPriority = 87
)

// Handler serves the Amazon Managed Service for Prometheus REST-JSON API and Prometheus data plane.
type Handler struct {
	Backend *InMemoryBackend
	server  *restroute.Server
	data    *restroute.Router
}

// NewHandler wraps a backend.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend, data: restroute.New(dataRoutes())}
	h.server = &restroute.Server{Router: restroute.New(routes()), Ops: h.ops()}

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "AMP" }

// Reset clears all state.
func (h *Handler) Reset() { h.Backend.Reset() }

// GetSupportedOperations lists the control-plane operations of the SDK client.
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
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher matches signed "aps" requests (or the SDK user agent) with a known path shape.
// Unsigned requests only match the Prometheus data-plane paths.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		r := c.Request()
		path := restroute.EscapedPath(r)

		if h.data.Handles(path) && httputils.ExtractServiceFromRequest(r) == "" {
			return true
		}

		if !h.server.Router.Handles(path) && !h.data.Handles(path) {
			return false
		}

		return httputils.ExtractServiceFromRequest(r) == signingName ||
			service.MatchesUserAgentMarker(r.Header, uaMarker)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return matchPriority }

func (h *Handler) operation(r *http.Request) (string, map[string]string, bool) {
	if op, params := h.data.Match(r.Method, restroute.EscapedPath(r)); op != "" {
		return op, params, true
	}

	op, params := h.server.Operation(r)

	return op, params, false
}

// ExtractOperation returns the operation name for metrics.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	op, _, _ := h.operation(c.Request())
	if op == "" {
		return "Unknown"
	}

	return op
}

// ExtractResource returns the workspace, scraper or ARN label for metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	_, params, _ := h.operation(c.Request())

	for _, k := range []string{keyWorkspaceID, paramScraperID, paramResource} {
		if v := params[k]; v != "" {
			return v
		}
	}

	return ""
}

type endpointBaseCtx = endpointBaseKey

func (h *Handler) requestContext(r *http.Request) context.Context {
	ctx := r.Context()
	if _, ok := awsmeta.Key.Get(ctx); ok {
		return ctx
	}

	return awsmeta.Set(ctx, awsmeta.FromRequest(r, h.Backend.Region()))
}

// Handler returns the Echo handler.
func (h *Handler) Handler() echo.HandlerFunc {
	serve := h.server.Handler()

	return func(c *echo.Context) error {
		ctx := h.requestContext(c.Request())
		if base, ok := requestBase(c.Scheme(), c.Request().Host); ok {
			ctx = context.WithValue(ctx, endpointBaseCtx{}, base)
		}

		c.SetRequest(c.Request().WithContext(ctx))

		op, params, data := h.operation(c.Request())
		if data {
			return h.serveData(c, op, params)
		}

		if op == "UpdateWorkspaceAlias" {
			return h.updateAliasNoContent(c, params)
		}

		return serve(c)
	}
}

// requestBase returns scheme://host for the address the caller reached the server on.
func requestBase(scheme, host string) (string, bool) {
	if scheme != "http" && scheme != "https" {
		return "", false
	}

	u, err := url.Parse(scheme + "://" + host)
	if err != nil || host == "" || u.Host != host || u.User != nil || u.Path != "" {
		return "", false
	}

	return scheme + "://" + host, true
}

// updateAliasNoContent serves the one operation whose success response is a bodyless 204.
func (h *Handler) updateAliasNoContent(c *echo.Context, params map[string]string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return restroute.WriteError(
			c,
			http.StatusBadRequest,
			"ValidationException",
			"Unable to read the request body",
			nil,
		)
	}

	req := &restroute.Request{Params: params, Query: c.Request().URL.Query(), Body: body}
	if _, _, ierr := h.updateWorkspaceAlias(c.Request().Context(), req); ierr != nil {
		return h.server.WriteErr(c, ierr)
	}

	return c.NoContent(http.StatusNoContent)
}
