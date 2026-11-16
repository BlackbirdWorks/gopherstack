package azurearm

import (
	_ "embed"
	"encoding/json/v2"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
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
	aciActionStart    = "start"
	aciActionStop     = "stop"
	aciActionRestart  = "restart"
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
	case aciActionStart, aciActionStop, aciActionRestart:
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
		match:  isACIProfileRevisions,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACIProfileRevisions(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPatch && isACIProfileScope(segs, aciProfileSegs)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACIProfilePatch(c, segs) },
	},
	{
		match: isACIOperations,
		handle: func(h *Handler, c *echo.Context, _ []string) error {
			return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: aciOperations()})
		},
	},
	{
		match:  isACILocationRead,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACILocationRead(c, segs) },
	},
	{
		match:  isACIAction,
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
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleACIExec(c, segs) },
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

func (h *Handler) handleACIExec(c *echo.Context, segs []string) error {
	p, id, ok := h.aciGroupFor(c, segs)
	if !ok {
		return nil
	}

	attach := strings.EqualFold(segs[len(segs)-1], "attach")

	var (
		command string
		term    ACITerminalSize
	)

	if !attach {
		body, err := decodeJSONBody(c.Request())
		if err != nil {
			return h.writeAPIError(c, err)
		}

		if command = strOf(body, "command"); command == "" {
			return h.writeError(c, http.StatusBadRequest, aciCodeInvalid, "The property 'command' is required.")
		}

		var terr error
		if term, terr = aciTerminalOf(body); terr != nil {
			return h.writeError(c, http.StatusBadRequest, aciCodeInvalid, terr.Error())
		}
	}

	uri, password, err := p.NewExecSession(c.Request().Host, id, segs[aciContainerIdx], command, attach, term)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{"webSocketUri": uri, "password": password})
}

//go:embed acicatalog/operations.json
var aciOperationsJSON []byte

// aciOperations lists the Microsoft.ContainerInstance operations from the Learn Azure permissions reference
// (role-based-access-control/permissions/containers); display.provider is the value of the OperationsList.json
// example of azure-rest-api-specs, and the undocumented display.resource/operation/origin members are omitted.
func aciOperations() []any {
	var doc struct {
		Value []any `json:"value"`
	}

	if err := json.Unmarshal(aciOperationsJSON, &doc, jsonx.V1()); err != nil {
		return []any{}
	}

	return doc.Value
}

func isACIOperations(segs []string, r *http.Request) bool {
	return r.Method == http.MethodGet && len(segs) == aciOpsSegs && segAt(segs, 0, providersSegment) &&
		segAt(segs, 1, namespaceMicrosoftContainerInstance) && segAt(segs, aciOpsNameIdx, "operations")
}

const (
	aciOpsSegs    = 3
	aciOpsNameIdx = 2
)
