package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	aciNsIdx          = 5
	aciTypeIdx        = 6
	aciGroupSegs      = 8
	aciActionSegs     = 9
	aciContainerSegs  = 11
	aciContainersIdx  = 8
	aciContainerIdx   = 9
	aciContainerOpIdx = 10
	aciSegContainers  = "containers"
)

func isACIScope(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, aciNsIdx, namespaceMicrosoftContainerInstance) && segAt(segs, aciTypeIdx, aciGroupsType)
}

func isACIAction(segs []string, r *http.Request) bool {
	if r.Method != http.MethodPost || !isACIScope(segs, aciActionSegs) {
		return false
	}

	switch strings.ToLower(segs[aciActionSegs-1]) {
	case "start", "stop", "restart":
		return true
	}

	return false
}

func isACIContainerOp(segs []string, r *http.Request, method string, ops ...string) bool {
	if r.Method != method || !isACIScope(segs, aciContainerSegs) || !segAt(segs, aciContainersIdx, aciSegContainers) {
		return false
	}

	for _, op := range ops {
		if strings.EqualFold(segs[aciContainerOpIdx], op) {
			return true
		}
	}

	return false
}

//nolint:gochecknoglobals // static route table, read-only after init
var containerInstanceRoutes = []armRoute{
	{
		match:  func(segs []string, r *http.Request) bool { return isACIAction(segs, r) },
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACIAction(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPatch && isACIScope(segs, aciGroupSegs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACIPatch(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isACIContainerOp(segs, r, http.MethodGet, "logs")
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACILogs(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isACIContainerOp(segs, r, http.MethodPost, "exec", "attach")
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error {
			return h.writeError(c, http.StatusNotImplemented, "NotImplemented",
				"Interactive exec and attach need a websocket terminal session, which this emulator does not provide.")
		},
	},
}

func (h *Handler) containerInstanceProvider() (*ContainerInstanceProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftContainerInstance)
	if !ok {
		return nil, false
	}

	cp, ok := p.(*ContainerInstanceProvider)

	return cp, ok
}

func (h *Handler) aciGroupFor(c *echo.Context, segs []string) (*ContainerInstanceProvider, ResourceID, bool) {
	p, ok := h.containerInstanceProvider()
	if !ok {
		_ = h.writeNotFound(c)

		return nil, ResourceID{}, false
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:aciGroupSegs]))
	if err != nil {
		_ = h.writeAPIError(c, err)

		return nil, ResourceID{}, false
	}

	return p, id, true
}

func (h *Handler) handleACIAction(c *echo.Context, segs []string) error {
	p, id, ok := h.aciGroupFor(c, segs)
	if !ok {
		return nil
	}

	if err := p.Action(c.Request().Context(), id, strings.ToLower(segs[aciActionSegs-1])); err != nil {
		return h.writeAPIError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleACIPatch(c *echo.Context, segs []string) error {
	p, id, ok := h.aciGroupFor(c, segs)
	if !ok {
		return nil
	}

	return h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) {
		return p.Patch(c.Request().Context(), id, body)
	})
}

func (h *Handler) handleACILogs(c *echo.Context, segs []string) error {
	p, id, ok := h.aciGroupFor(c, segs)
	if !ok {
		return nil
	}

	content, err := p.Logs(c.Request().Context(), id, segs[aciContainerIdx], aciTailOf(c.QueryParam("tail")))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{"content": content})
}
