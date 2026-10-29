package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	acaNsIdx        = 5
	acaTypeIdx      = 6
	acaInstSegs     = 8
	acaActionSegs   = 9
	acaRevActSegs   = 11
	acaRevSegIdx    = 8
	acaExecActSegs  = 11
	acaExecSegIdx   = 8
	acaActionStart  = "start"
	acaActionStop   = "stop"
	acaActionSecret = "listsecrets"
)

func isACAScope(segs []string, typ string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, acaNsIdx, namespaceMicrosoftApp) && segAt(segs, acaTypeIdx, typ)
}

func acaLast(segs []string) string { return strings.ToLower(segs[len(segs)-1]) }

//nolint:gochecknoglobals // static route table, read-only after init
var containerAppsRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isNamespaceOperation(segs, namespaceMicrosoftApp)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleNSOperation(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			if r.Method != http.MethodPost || !isACAScope(segs, acaAppType, acaActionSegs) {
				return false
			}

			switch acaLast(segs) {
			case acaActionStart, acaActionStop, acaActionSecret:
				return true
			}

			return false
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACAAppAction(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			if r.Method != http.MethodPost || !isACAScope(segs, acaAppType, acaRevActSegs) ||
				!segAt(segs, acaRevSegIdx, acaRevisionType) {
				return false
			}

			switch acaLast(segs) {
			case acaActActivate, acaActDeactivate, acaActRestart:
				return true
			}

			return false
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACARevisionAction(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			if r.Method != http.MethodPost {
				return false
			}

			switch {
			case isACAScope(segs, acaJobType, acaActionSegs):
				switch acaLast(segs) {
				case acaActionStart, acaActionStop, acaActionSecret:
					return true
				}
			case isACAScope(segs, acaJobType, acaExecActSegs):
				return segAt(segs, acaExecSegIdx, acaExecutionType) && acaLast(segs) == acaActionStop
			}

			return false
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACAJobAction(c, segs) },
	},
	{
		match: func(segs []string, _ *http.Request) bool { return isNamespaceScope(segs, namespaceMicrosoftApp) },
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleNamespace(c, segs, namespaceMicrosoftApp)
		},
	},
}

func (h *Handler) containerAppsProvider() (*ContainerAppsProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftApp)
	if !ok {
		return nil, false
	}

	cp, ok := p.(*ContainerAppsProvider)

	return cp, ok
}

func (h *Handler) acaFor(c *echo.Context, segs []string) (*ContainerAppsProvider, ResourceID, bool) {
	p, ok := h.containerAppsProvider()
	if !ok {
		_ = h.writeNotFound(c)

		return nil, ResourceID{}, false
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:acaInstSegs]))
	if err != nil {
		_ = h.writeAPIError(c, err)

		return nil, ResourceID{}, false
	}

	return p, id, true
}

func (h *Handler) handleACAAppAction(c *echo.Context, segs []string) error {
	p, id, ok := h.acaFor(c, segs)
	if !ok {
		return nil
	}

	var (
		out map[string]any
		err error
	)

	switch acaLast(segs) {
	case acaActionStart:
		out, err = p.setStopped(c.Request().Context(), id, false)
	case acaActionStop:
		out, err = p.setStopped(c.Request().Context(), id, true)
	default:
		out, err = p.listSecrets(id)
	}

	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) handleACARevisionAction(c *echo.Context, segs []string) error {
	p, id, ok := h.acaFor(c, segs)
	if !ok {
		return nil
	}

	if err := p.revisionAction(c.Request().Context(), id, segs[acaRevSegIdx+1], acaLast(segs)); err != nil {
		return h.writeAPIError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleACAJobAction(c *echo.Context, segs []string) error {
	p, id, ok := h.acaFor(c, segs)
	if !ok {
		return nil
	}

	switch {
	case len(segs) == acaExecActSegs:
		stopped, err := p.stopExecutions(c.Request().Context(), id, segs[acaExecSegIdx+1])
		if err != nil {
			return h.writeAPIError(c, err)
		}

		out := map[string]any{}
		if len(stopped) > 0 {
			out = stopped[0]
		}

		return h.writeJSON(c, http.StatusOK, out)
	case acaLast(segs) == acaActionSecret:
		return h.writeACAResult(c, func() (map[string]any, error) { return p.listJobSecrets(id) })
	case acaLast(segs) == acaActionStop:
		stopped, err := p.stopExecutions(c.Request().Context(), id, "")
		if err != nil {
			return h.writeAPIError(c, err)
		}

		if stopped == nil {
			stopped = []map[string]any{}
		}

		return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: stopped})
	}

	return h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) { return p.startJob(id, body) })
}

func (h *Handler) writeACAResult(c *echo.Context, f func() (map[string]any, error)) error {
	out, err := f()
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}
