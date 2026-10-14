package arczonalshift

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
	signingName   = "arc-zonal-shift"
	uaMarker      = "api/arczonalshift"
	matchPriority = 87
)

type regionKey struct{}

// Handler serves the ARC zonal shift API.
type Handler struct {
	Backend       *InMemoryBackend
	server        *restroute.Server
	DefaultRegion string
}

// NewHandler creates the handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	h := &Handler{Backend: backend}
	h.server = &restroute.Server{
		Router: restroute.New(routes()),
		Ops:    h.ops(),
		Context: func(r *http.Request) context.Context {
			return context.WithValue(r.Context(), regionKey{}, httputils.ExtractRegionFromRequest(r, h.DefaultRegion))
		},
	}

	return h
}

// Name returns the service name.
func (h *Handler) Name() string { return "ARCZonalShift" }

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
	g, p, d, u, pa := http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodPut, http.MethodPatch

	return []restroute.Route{
		{Method: p, Path: "/zonalshifts", Op: "StartZonalShift"},
		{Method: g, Path: "/zonalshifts", Op: "ListZonalShifts"},
		{Method: pa, Path: "/zonalshifts/{zonalShiftId}", Op: "UpdateZonalShift"},
		{Method: d, Path: "/zonalshifts/{zonalShiftId}", Op: "CancelZonalShift"},
		{Method: p, Path: "/practiceruns", Op: "StartPracticeRun"},
		{Method: d, Path: "/practiceruns/{zonalShiftId}", Op: "CancelPracticeRun"},
		{Method: p, Path: "/configuration", Op: "CreatePracticeRunConfiguration"},
		{Method: pa, Path: "/configuration/{resourceIdentifier}", Op: "UpdatePracticeRunConfiguration"},
		{Method: d, Path: "/configuration/{resourceIdentifier}", Op: "DeletePracticeRunConfiguration"},
		{Method: g, Path: "/managedresources", Op: "ListManagedResources"},
		{Method: g, Path: "/managedresources/{resourceIdentifier}", Op: "GetManagedResource"},
		{Method: u, Path: "/managedresources/{resourceIdentifier}", Op: "UpdateZonalAutoshiftConfiguration"},
		{Method: g, Path: "/autoshifts", Op: "ListAutoshifts"},
		{Method: g, Path: "/autoshift-observer-notification", Op: "GetAutoshiftObserverNotificationStatus"},
		{Method: u, Path: "/autoshift-observer-notification", Op: "UpdateAutoshiftObserverNotificationStatus"},
	}
}

func (h *Handler) ops() map[string]restroute.OpFunc {
	return map[string]restroute.OpFunc{
		"StartZonalShift":                           h.startZonalShift,
		"ListZonalShifts":                           h.listZonalShifts,
		"UpdateZonalShift":                          h.updateZonalShift,
		"CancelZonalShift":                          h.cancelZonalShift,
		"StartPracticeRun":                          h.startPracticeRun,
		"CancelPracticeRun":                         h.cancelPracticeRun,
		"CreatePracticeRunConfiguration":            h.createPractice,
		"UpdatePracticeRunConfiguration":            h.updatePractice,
		"DeletePracticeRunConfiguration":            h.deletePractice,
		"ListManagedResources":                      h.listManagedResources,
		"GetManagedResource":                        h.getManagedResource,
		"UpdateZonalAutoshiftConfiguration":         h.updateAutoshift,
		"ListAutoshifts":                            h.listAutoshifts,
		"GetAutoshiftObserverNotificationStatus":    h.getObserver,
		"UpdateAutoshiftObserverNotificationStatus": h.updateObserver,
	}
}
