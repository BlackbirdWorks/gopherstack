package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	cmpRGTypeIdx    = 6
	cmpRGNameIdx    = 7
	cmpRGActionIdx  = 8
	cmpRGActionSegs = 9
	cmpSubLocSegs   = 7
	cmpSubLocIdx    = 4
	cmpSubTypeSegs  = 5
	cmpActionVMSize = "vmSizes"

	cmpQueryStatusOnly = "statusOnly"
)

//nolint:gochecknoglobals // static route table, read-only after init
var computeRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isNamespaceOperation(segs, namespaceMicrosoftCompute)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleNSOperation(c, segs) },
	},
	{
		match:  func(segs []string, _ *http.Request) bool { return isNamespaceScope(segs, namespaceMicrosoftCompute) },
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleCompute(c, segs) },
	},
}

func (h *Handler) computeProvider() (*ComputeProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftCompute)
	if !ok {
		return nil, false
	}

	cp, ok := p.(*ComputeProvider)

	return cp, ok
}

func (h *Handler) handleCompute(c *echo.Context, segs []string) error {
	cp, ok := h.computeProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	if !h.checkNSAPIVersion(c, namespaceMicrosoftCompute, computeAPIVersions(), namespaceMicrosoftCompute) {
		return nil
	}

	if handled, err := h.computeCatalogScope(c, segs); handled {
		return err
	}

	if handled, err := h.computeSpecial(c, cp, segs); handled {
		return err
	}

	t, err := resolveNSTarget(segs)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if canonicalCmpType(firstType(t.id)) == "" {
		return h.handleLegacyNamespace(c, segs, t)
	}

	return h.nsDispatch(c, t, segs)
}

// computeSpecial serves the Compute shapes that are not plain list/PUT/GET/DELETE/PATCH.
func (h *Handler) computeSpecial(c *echo.Context, cp *ComputeProvider, segs []string) (bool, error) {
	method := c.Request().Method

	if !segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) {
		return h.computeSubscriptionScope(c, segs)
	}

	switch {
	case len(segs) == cmpRGActionSegs && method == http.MethodGet:
		return h.computeGetSub(c, cp, segs)
	case len(segs) == cmpRGActionSegs && method == http.MethodPost:
		return true, h.computeAction(c, cp, segs)
	case len(segs) == cmpRGActionIdx && method == http.MethodGet &&
		strings.EqualFold(c.QueryParam("$expand"), cmpFieldInstView):
		return true, h.computeGetExpanded(c, cp, segs)
	case len(segs) == cmpRGNameIdx && method == http.MethodGet && c.QueryParam(cmpQueryStatusOnly) == cmpTrue:
		return true, h.computeStatusOnly(c, cp, segs)
	}

	return h.computeChildScope(c, cp, segs)
}

func (h *Handler) computeSubscriptionScope(c *echo.Context, segs []string) (bool, error) {
	if c.Request().Method != http.MethodGet {
		return false, nil
	}

	switch {
	case len(segs) == cmpSubTypeSegs && segAt(segs, cmpSubLocIdx, "skus"):
		return true, h.writeJSON(c, http.StatusOK, ResourceSKUs(skuLocationFilter(c.QueryParam("$filter"))))
	case len(segs) == cmpSubLocSegs && segAt(segs, cmpSubLocIdx, cmpKeyLocations) &&
		segAt(segs, cmpSubLocSegs-1, cmpActionVMSize):
		return true, h.writeJSON(c, http.StatusOK, VMSizes())
	case len(segs) == cmpSubLocSegs && segAt(segs, cmpSubLocIdx, cmpKeyLocations) &&
		(segAt(segs, cmpSubLocSegs-1, cmpVMs) || segAt(segs, cmpSubLocSegs-1, cmpVMSS)):
		return true, h.computeListByLocation(c, segs)
	case len(segs) == cmpSubTypeSegs && segAt(segs, cmpSubLocIdx, cmpVMs) && c.QueryParam(cmpQueryStatusOnly) == cmpTrue:
		cp, ok := h.computeProvider()
		if !ok {
			return true, h.writeNotFound(c)
		}

		return true, h.computeListStatus(c, cp, ResourceID{
			SubscriptionID: segs[1], Namespace: namespaceMicrosoftCompute, Types: []string{cmpVMs},
		})
	}

	return false, nil
}

// skuLocationFilter extracts the location from a "location eq 'westus'" filter.
func skuLocationFilter(filter string) string {
	f := strings.ToLower(filter)

	_, rest, found := strings.Cut(f, "location eq")
	if !found {
		return ""
	}

	return strings.Trim(strings.TrimSpace(rest), "' ")
}

func (h *Handler) computeID(segs []string) (ResourceID, error) {
	return ParseGenericResourcePath("/" + joinSegs(segs[:cmpRGActionIdx]))
}

func (h *Handler) computeGetSub(c *echo.Context, cp *ComputeProvider, segs []string) (bool, error) {
	id, err := h.computeID(segs)
	if err != nil {
		return true, h.writeAPIError(c, err)
	}

	sub := segs[cmpRGActionIdx]

	switch {
	case strings.EqualFold(firstType(id), cmpVMs) && strings.EqualFold(sub, cmpFieldInstView):
		out, ierr := cp.InstanceView(id)
		if ierr != nil {
			return true, h.writeAPIError(c, ierr)
		}

		return true, h.writeJSON(c, http.StatusOK, out)
	case strings.EqualFold(sub, cmpActionVMSize) &&
		(strings.EqualFold(firstType(id), cmpVMs) || strings.EqualFold(firstType(id), cmpAvSets)):
		if _, gerr := cp.Get(c.Request().Context(), id); gerr != nil {
			return true, h.writeAPIError(c, gerr)
		}

		return true, h.writeJSON(c, http.StatusOK, VMSizes())
	}

	return false, nil
}

func (h *Handler) computeGetExpanded(c *echo.Context, cp *ComputeProvider, segs []string) error {
	id, err := ParseGenericResourcePath("/" + joinSegs(segs))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if !strings.EqualFold(firstType(id), cmpVMs) || len(id.Types) != 1 {
		return h.getGenericResource(c, id)
	}

	out, err := cp.GetExpanded(id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) computeStatusOnly(c *echo.Context, cp *ComputeProvider, segs []string) error {
	t, err := resolveNSTarget(segs)
	if err != nil || !strings.EqualFold(firstType(t.id), cmpVMs) {
		return h.nsDispatch(c, t, segs)
	}

	return h.computeListStatus(c, cp, t.id)
}

func (h *Handler) computeListStatus(c *echo.Context, cp *ComputeProvider, id ResourceID) error {
	values, err := cp.list(id, true)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, h.pageNS(c, values, nsDefaultTop(id.Namespace)))
}

func (h *Handler) computeAction(c *echo.Context, cp *ComputeProvider, segs []string) error {
	id, err := h.computeID(segs)
	if err != nil || len(id.Types) != 1 {
		return h.writeNotFound(c)
	}

	action := segs[cmpRGActionIdx]

	var actErr error

	switch strings.ToLower(firstType(id)) {
	case strings.ToLower(cmpVMs):
		actErr = h.vmAction(c, cp, id, action)
	case strings.ToLower(cmpVMSS):
		actErr = h.vmssAction(c, cp, id, action)
	default:
		actErr = cmpNotImplemented(firstType(id) + "/" + action)
	}

	if actErr != nil {
		return h.writeAPIError(c, actErr)
	}

	if strings.EqualFold(action, vmActionGeneralize) {
		return c.NoContent(http.StatusOK)
	}

	h.setNSOperation(c, id, true)

	return c.NoContent(http.StatusAccepted)
}

func (h *Handler) vmssAction(c *echo.Context, cp *ComputeProvider, id ResourceID, action string) error {
	var body map[string]any

	if c.Request().ContentLength != 0 {
		decoded, err := decodeJSONBody(c.Request())
		if err != nil {
			return err
		}

		body = decoded
	}

	return cp.VMSSAction(id, action, strList(body["instanceIds"]))
}

func (h *Handler) computeListByLocation(c *echo.Context, segs []string) error {
	cp, ok := h.computeProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	id := ResourceID{
		SubscriptionID: segs[1],
		Namespace:      namespaceMicrosoftCompute,
		Types:          []string{canonicalCmpType(segs[cmpSubLocSegs-1])},
	}

	values, err := cp.list(id, false)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	want := normLocation(segs[cmpSubLocIdx+1])
	filtered := values[:0:0]

	for _, v := range values {
		if normLocation(strOf(v, fieldLocation)) == want {
			filtered = append(filtered, v)
		}
	}

	return h.writeJSON(c, http.StatusOK, h.pageNS(c, filtered, nsDefaultTop(id.Namespace)))
}
