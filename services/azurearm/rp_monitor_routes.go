package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	monitorWorkspaceLen   = 8
	monitorActionLen      = 9
	monitorTableLen       = 10
	monitorNamespaceIdx   = 5
	monitorTypeIdx        = 6
	monitorTableTypeIdx   = 8
	monitorGroupsIdx      = 2
	monitorProvidersIdx   = 4
	actionSharedKeys      = "sharedKeys"
	actionRegenerateShare = "regenerateSharedKey"
)

func isMonitorPath(segs []string, n int, ns, typ string) bool {
	return len(segs) == n && isSubscriptionScoped(segs) && segAt(segs, monitorGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, monitorProvidersIdx, providersSegment) && segAt(segs, monitorNamespaceIdx, ns) &&
		segAt(segs, monitorTypeIdx, typ)
}

func isMonitorWorkspaceChild(segs []string, n int) bool {
	return isMonitorPath(segs, n, namespaceMicrosoftOperationalInsights, monitorWorkspacesType)
}

//nolint:gochecknoglobals // static route table, read-only after init
var monitorRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPatch && (isMonitorWorkspaceChild(segs, monitorWorkspaceLen) ||
				isMonitorPath(segs, monitorWorkspaceLen, namespaceMicrosoftInsights, monitorDCEType) ||
				isMonitorPath(segs, monitorWorkspaceLen, namespaceMicrosoftInsights, monitorDCRType) ||
				(isMonitorWorkspaceChild(segs, monitorTableLen) && segAt(segs, monitorTableTypeIdx, monitorTablesType)))
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleMonitorPatch(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPost && isMonitorWorkspaceChild(segs, monitorActionLen) &&
				(strings.EqualFold(segs[8], actionSharedKeys) || strings.EqualFold(segs[8], actionRegenerateShare))
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleMonitorAction(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isMonitorWorkspaceChild(segs, monitorActionLen) &&
				segAt(segs, monitorTableTypeIdx, monitorTablesType)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleMonitorListTables(c, segs) },
	},
}

func (h *Handler) monitorProvider(ns string) (*MonitorProvider, bool) {
	p, ok := h.Registry.ProviderNamed(ns)
	if !ok {
		return nil, false
	}

	mp, ok := p.(*MonitorProvider)

	return mp, ok
}

func (h *Handler) monitorProviderFor(c *echo.Context, segs []string) (*MonitorProvider, ResourceID, error) {
	id, err := ParseGenericResourcePath("/" + joinSegs(segs))
	if err != nil {
		return nil, id, h.writeAPIError(c, err)
	}

	p, ok := h.monitorProvider(id.Namespace)
	if !ok {
		return nil, id, h.writeNotFound(c)
	}

	return p, id, nil
}

func (h *Handler) handleMonitorPatch(c *echo.Context, segs []string) error {
	p, id, err := h.monitorProviderFor(c, segs)
	if p == nil {
		return err
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := p.Patch(c.Request().Context(), id, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleMonitorAction(c *echo.Context, segs []string) error {
	p, id, err := h.monitorProviderFor(c, segs[:len(segs)-1])
	if p == nil {
		return err
	}

	out, err := p.Action(c.Request().Context(), id, segs[len(segs)-1], nil)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleMonitorListTables(c *echo.Context, segs []string) error {
	p, id, err := h.monitorProviderFor(c, segs[:len(segs)-1])
	if p == nil {
		return err
	}

	id.Types = append(id.Types, monitorTablesType)

	out, err := p.List(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: out})
}
