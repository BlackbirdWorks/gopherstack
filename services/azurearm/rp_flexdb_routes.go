package azurearm

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	flexNsIdx         = 5
	flexTypeIdx       = 6
	flexServerSegs    = 8
	flexListSegs      = 9
	flexChildSegs     = 10
	flexLocOpSegs     = 8
	flexLocSegs       = 7
	flexSubSegs       = 5
	flexOpNsIdx       = 3
	flexOpKindIdx     = 6
	flexLocSegIdx     = 4
	flexProvidersIdx  = 2
	flexSubNsIdx      = 3
	flexOpAsync       = "azureAsyncOperation"
	flexOpProgress    = "operationProgress"
	flexOpResults     = "operationResults"
	flexStatusOK      = "Succeeded"
	flexStatusRunning = "InProgress"
	flexStatusFailed  = "Failed"
	flexRetryMS       = "100"
)

func flexNS(s string) bool {
	return strings.EqualFold(s, namespaceDBforPostgreSQL) || strings.EqualFold(s, namespaceDBforMySQL)
}

func isFlexScope(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		flexNS(segs[flexNsIdx]) && segAt(segs, flexTypeIdx, flexServersType)
}

func isFlexChildType(segs []string) bool {
	switch strings.ToLower(segs[flexServerSegs]) {
	case strings.ToLower(flexFirewallType), strings.ToLower(flexDatabasesType), strings.ToLower(flexConfigsType):
		return true
	}

	return false
}

func isFlexLocationOp(segs []string) bool {
	if len(segs) != flexLocOpSegs || !isSubscriptionScoped(segs) || !segAt(segs, flexProvidersIdx, providersSegment) ||
		!flexNS(segs[flexOpNsIdx]) || !segAt(segs, flexLocSegIdx, "locations") {
		return false
	}

	switch strings.ToLower(segs[flexOpKindIdx]) {
	case strings.ToLower(flexOpAsync), strings.ToLower(flexOpProgress), strings.ToLower(flexOpResults):
		return true
	}

	return false
}

func isFlexWrite(segs []string, r *http.Request) bool {
	switch r.Method {
	case http.MethodPut, http.MethodDelete:
	case http.MethodPatch:
		patchable := isFlexScope(segs, flexServerSegs) ||
			(isFlexScope(segs, flexChildSegs) && segAt(segs, flexServerSegs, flexConfigsType))
		if !patchable {
			return false
		}
	default:
		return false
	}

	return isFlexScope(segs, flexServerSegs) || (isFlexScope(segs, flexChildSegs) && isFlexChildType(segs))
}

func isFlexAction(segs []string, r *http.Request) bool {
	if r.Method != http.MethodPost || !isFlexScope(segs, flexListSegs) {
		return false
	}

	switch strings.ToLower(segs[flexServerSegs]) {
	case aksActionStart, aksActionStop, "restart":
		return true
	}

	return false
}

func isFlexList(segs []string, r *http.Request) bool {
	if r.Method != http.MethodGet || !isFlexScope(segs, flexListSegs) {
		return false
	}

	return isFlexChildType(segs) || strings.EqualFold(segs[flexServerSegs], flexReplicasType)
}

func isFlexSubOp(segs []string, r *http.Request, n int, last string, method string) bool {
	return r.Method == method && len(segs) == n && isSubscriptionScoped(segs) &&
		segAt(segs, flexProvidersIdx, providersSegment) &&
		flexNS(segs[flexSubNsIdx]) &&
		strings.EqualFold(segs[n-1], last) &&
		(n == flexSubSegs || segAt(segs, flexLocSegIdx, "locations"))
}

//nolint:gochecknoglobals // static route table, read-only after init
var flexRoutes = []armRoute{
	{
		match:  func(segs []string, r *http.Request) bool { return r.Method == http.MethodGet && isFlexLocationOp(segs) },
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFlexOperation(c, segs) },
	},
	{
		match:  isFlexWrite,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFlexWrite(c, segs) },
	},
	{
		match:  isFlexAction,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFlexAction(c, segs) },
	},
	{
		match:  isFlexList,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFlexList(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isFlexSubOp(segs, r, flexSubSegs, "checkNameAvailability", http.MethodPost) ||
				isFlexSubOp(segs, r, flexLocSegs, "checkNameAvailability", http.MethodPost)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFlexCheckName(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isFlexSubOp(segs, r, flexLocSegs, "capabilities", http.MethodGet)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFlexCapabilities(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isFlexSubOp(segs, r, flexLocSegs, "capabilitySets", http.MethodGet) ||
				(r.Method == http.MethodGet && len(segs) == flexLocOpSegs && isSubscriptionScoped(segs) &&
					flexNS(segs[flexSubNsIdx]) && segAt(segs, flexLocSegIdx, "locations") &&
					strings.EqualFold(segs[flexOpKindIdx], "capabilitySets"))
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleFlexCapabilitySets(c, segs) },
	},
}

func (h *Handler) flexProviderFor(ns string) (*FlexDBProvider, bool) {
	p, ok := h.Registry.ProviderNamed(ns)
	if !ok {
		return nil, false
	}

	fp, ok := p.(*FlexDBProvider)

	return fp, ok
}

// setFlexOperation points the response's Azure-AsyncOperation and Location headers at status resources.
func (h *Handler) setFlexOperation(c *echo.Context, p *FlexDBProvider, sub, opID string) {
	asyncKind := flexOpAsync
	if p.flavor.kind == flexKindMySQL {
		asyncKind = flexOpProgress
	}

	base := h.baseURLFor(c.Request()) + "/subscriptions/" + sub + "/providers/" + p.flavor.ns + "/locations/" +
		DefaultLocation + "/"
	query := "?api-version=" + c.Request().URL.Query().Get("api-version")

	hdr := c.Response().Header()
	hdr.Set("Azure-Asyncoperation", base+asyncKind+"/"+opID+query)
	hdr.Set("Location", base+flexOpResults+"/"+opID+query)
	hdr.Set("Retry-After", "1")
	hdr.Set("Retry-After-Ms", flexRetryMS)
}

func (h *Handler) handleFlexOperation(c *echo.Context, segs []string) error {
	p, ok := h.flexProviderFor(segs[flexOpNsIdx])
	if !ok {
		return h.writeNotFound(c)
	}

	opID := segs[len(segs)-1]

	op, known, done := p.OperationStatus(opID)
	if !known {
		return h.writeError(c, http.StatusNotFound, armErrorCodeResourceNotFound, "The operation was not found.")
	}

	if !strings.EqualFold(segs[flexOpKindIdx], flexOpResults) {
		return h.writeJSON(c, http.StatusOK, flexOpStatus(opID, op, done))
	}

	switch {
	case !done:
		return c.NoContent(http.StatusAccepted)
	case op.ErrMsg != "":
		return h.writeError(c, http.StatusInternalServerError, "InternalServerError", op.ErrMsg)
	case !op.Returns:
		return h.writeJSON(c, http.StatusOK, map[string]any{})
	}

	id, err := ParseGenericResourcePath(op.Target)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	body, err := p.Get(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, body)
}

func flexOpStatus(opID string, op flexOp, done bool) map[string]any {
	out := map[string]any{
		fieldName: opID, acrPropStatus: flexStatusRunning, "startTime": op.Started.UTC().Format(time.RFC3339Nano),
	}

	switch {
	case done && op.ErrMsg != "":
		out[acrPropStatus] = flexStatusFailed
		out["error"] = map[string]any{aksFieldCode: "InternalServerError", fieldMessage: op.ErrMsg}
		out["endTime"] = op.ReadyAt.UTC().Format(time.RFC3339Nano)
	case done:
		out[acrPropStatus] = flexStatusOK
		out["endTime"] = op.ReadyAt.UTC().Format(time.RFC3339Nano)
	}

	return out
}

func (h *Handler) flexFor(c *echo.Context, segs []string) (*FlexDBProvider, ResourceID, error) {
	p, ok := h.flexProviderFor(segs[flexNsIdx])
	if !ok {
		return nil, ResourceID{}, h.writeNotFound(c)
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs))
	if err != nil {
		return nil, ResourceID{}, h.writeAPIError(c, err)
	}

	return p, id, nil
}

func (h *Handler) handleFlexWrite(c *echo.Context, segs []string) error {
	p, id, err := h.flexFor(c, segs)
	if p == nil {
		return err
	}

	method := c.Request().Method
	if method == http.MethodDelete {
		return h.deleteFlex(c, p, id)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	_, getErr := p.Get(c.Request().Context(), id)
	status := http.StatusOK

	if getErr != nil && method == http.MethodPut {
		status = http.StatusCreated
	}

	res, err := p.Write(id, method, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	h.setFlexOperation(c, p, id.SubscriptionID, res.opID)

	return h.writeJSON(c, status, res.body)
}

func (h *Handler) deleteFlex(c *echo.Context, p *FlexDBProvider, id ResourceID) error {
	opID, err := p.Remove(id)
	if err != nil {
		if fe, ok := errors.AsType[*flexError](err); ok && fe.status == http.StatusNotFound {
			return c.NoContent(http.StatusNoContent)
		}

		return h.writeAPIError(c, err)
	}

	h.setFlexOperation(c, p, id.SubscriptionID, opID)

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleFlexAction(c *echo.Context, segs []string) error {
	p, id, err := h.flexFor(c, segs[:len(segs)-1])
	if p == nil {
		return err
	}

	opID, err := p.Action(id, strings.ToLower(segs[len(segs)-1]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	h.setFlexOperation(c, p, id.SubscriptionID, opID)

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) handleFlexList(c *echo.Context, segs []string) error {
	p, id, err := h.flexFor(c, segs[:len(segs)-1])
	if p == nil {
		return err
	}

	values, err := p.ListChildren(id, segs[len(segs)-1])
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: values})
}

func (h *Handler) handleFlexCheckName(c *echo.Context, segs []string) error {
	p, ok := h.flexProviderFor(segs[flexSubNsIdx])
	if !ok {
		return h.writeNotFound(c)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	name, _ := flexStr(body, fieldName)
	typ, _ := flexStr(body, fieldType)

	return h.writeJSON(c, http.StatusOK, p.CheckName(c.Request().Context(), name, typ))
}

func (h *Handler) handleFlexCapabilities(c *echo.Context, segs []string) error {
	p, ok := h.flexProviderFor(segs[flexSubNsIdx])
	if !ok {
		return h.writeNotFound(c)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: p.Capabilities()})
}

func (h *Handler) handleFlexCapabilitySets(c *echo.Context, segs []string) error {
	p, ok := h.flexProviderFor(segs[flexSubNsIdx])
	if !ok || p.flavor.kind != flexKindMySQL {
		return h.writeNotFound(c)
	}

	set := p.CapabilitySet(segs[1])

	if len(segs) == flexLocOpSegs {
		if !strings.EqualFold(segs[len(segs)-1], aksDefaultName) {
			return h.writeError(
				c,
				http.StatusNotFound,
				armErrorCodeResourceNotFound,
				"The capability set was not found.",
			)
		}

		return h.writeJSON(c, http.StatusOK, set)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: []any{set}})
}
