package azurearm

import (
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
)

const (
	nsLocationsIdx    = 4
	nsSubNamespaceIdx = 3
	nsRGNamespaceIdx  = 5
	nsOpKindIdx       = 6
	nsOpSegs          = 8
	nsListSegs        = 5
	nsRGListSegs      = 7
	nsRGInstanceSegs  = 8
	nsAPIVersionKey   = "api-version"
	nsOperations      = "operations"
	nsOperationRes    = "operationResults"
	nsCheckIPAction   = "CheckIPAddressAvailability"
	nsCheckIPSegs     = 9
	apiVersionMinYr   = 2015
	apiVersionMaxYr   = 2026
)

var apiVersionRe = regexp.MustCompile(`^(\d{4})-\d{2}-\d{2}(-preview)?$`)

// isNamespaceScope matches subscription- and resource-group-scoped paths under namespace ns.
func isNamespaceScope(segs []string, ns string) bool {
	if len(segs) < nsListSegs || !isSubscriptionScoped(segs) {
		return false
	}

	if segAt(segs, providersOrResourceGroupsIdx, providersSegment) {
		return segAt(segs, nsSubNamespaceIdx, ns)
	}

	return len(segs) >= nsRGListSegs && segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) && segAt(segs, nsRGNamespaceIdx, ns)
}

func isNamespaceOperation(segs []string, ns string) bool {
	return len(segs) == nsOpSegs && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, providersSegment) &&
		segAt(segs, nsSubNamespaceIdx, ns) &&
		segAt(segs, nsLocationsIdx, "locations") &&
		(segAt(segs, nsOpKindIdx, nsOperations) || segAt(segs, nsOpKindIdx, nsOperationRes))
}

//nolint:gochecknoglobals // static route table, read-only after init
var networkRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet && isNamespaceOperation(segs, namespaceMicrosoftNetwork)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleNSOperation(c, segs) },
	},
	{
		match: func(segs []string, _ *http.Request) bool { return isNamespaceScope(segs, namespaceMicrosoftNetwork) },
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleNamespace(c, segs, namespaceMicrosoftNetwork)
		},
	},
}

//nolint:gochecknoglobals // static route table, read-only after init
var managedIdentityRoutes = []armRoute{
	{
		match: func(segs []string, _ *http.Request) bool {
			return isNamespaceScope(segs, namespaceMicrosoftManagedIdentity)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleNamespace(c, segs, namespaceMicrosoftManagedIdentity)
		},
	},
}

func (h *Handler) handleNSOperation(c *echo.Context, segs []string) error {
	if segAt(segs, nsOpKindIdx, nsOperationRes) {
		return c.NoContent(http.StatusNoContent)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{netPropStatus: netStatusSucceeded, fieldName: segs[nsOpSegs-1]})
}

// checkNSAPIVersion enforces the api-version query parameter the way ARM does.
func (h *Handler) checkNSAPIVersion(c *echo.Context, ns string, supported []string, typ string) bool {
	v := c.QueryParam(nsAPIVersionKey)
	if v == "" {
		_ = h.writeError(c, http.StatusBadRequest, "MissingApiVersionParameter",
			"The api-version query parameter (?api-version=) is required for all requests.")

		return false
	}

	m := apiVersionRe.FindStringSubmatch(v)
	if m == nil {
		_ = h.writeError(c, http.StatusBadRequest, "InvalidApiVersionParameter",
			"The api-version '"+v+"' is invalid. The supported versions are '"+strings.Join(supported, ",")+"'.")

		return false
	}

	year, _ := strconv.Atoi(m[1])
	if ns == namespaceMicrosoftManagedIdentity {
		if !containsFold(supported, v) {
			year = 0
		}
	}

	if year < apiVersionMinYr || year > apiVersionMaxYr {
		_ = h.writeError(
			c,
			http.StatusBadRequest,
			"NoRegisteredProviderFound",
			"No registered resource provider found for location '"+DefaultLocation+"' and API version '"+v+"' for type '"+
				typ+"'. The supported api-versions are '"+strings.Join(
				supported,
				", ",
			)+"'.",
		)

		return false
	}

	return true
}

func containsFold(list []string, v string) bool {
	for _, s := range list {
		if strings.EqualFold(s, v) {
			return true
		}
	}

	return false
}

// nsTarget is what a namespace request resolves to.
type nsTarget struct {
	id       ResourceID
	listing  bool
	instance bool
}

func resolveNSTarget(segs []string) (nsTarget, error) {
	switch {
	case len(segs) == nsListSegs || len(segs) == nsRGListSegs:
		id, _, err := ParseGenericResourceListPath("/" + joinSegs(segs))

		return nsTarget{id: id, listing: true}, err
	case len(segs)%2 == 0:
		id, err := ParseGenericResourcePath("/" + joinSegs(segs))

		return nsTarget{id: id, instance: true}, err
	default:
		parent, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
		parent.Types = append(parent.Types, segs[len(segs)-1])

		return nsTarget{id: parent, listing: true}, err
	}
}

func (h *Handler) handleNamespace(c *echo.Context, segs []string, ns string) error {
	t, err := resolveNSTarget(segs)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if ns == namespaceMicrosoftNetwork && canonicalTop(firstType(t.id)) == "" {
		return h.handleLegacyNamespace(c, segs, t)
	}

	if !h.checkNSAPIVersion(c, ns, namespaceAPIVersions(ns), ns+"/"+firstType(t.id)) {
		return nil
	}

	return h.nsDispatch(c, t, segs)
}

func namespaceAPIVersions(ns string) []string {
	switch ns {
	case namespaceMicrosoftManagedIdentity:
		return msiAPIVersions()
	case namespaceMicrosoftCompute:
		return computeAPIVersions()
	case namespaceMicrosoftApp:
		return acaAPIVersions()
	case namespaceMicrosoftContainerService:
		return aksAPIVersions()
	}

	return networkAPIVersions()
}

// nsDispatch serves the generic list/PUT/GET/DELETE/PATCH shapes once the api-version has been checked.
func (h *Handler) nsDispatch(c *echo.Context, t nsTarget, segs []string) error {
	if t.listing {
		return h.handleNSList(c, t.id, segs)
	}

	switch c.Request().Method {
	case http.MethodPut:
		return h.handleNSPut(c, t.id)
	case http.MethodGet:
		return h.getGenericResource(c, t.id)
	case http.MethodDelete:
		return h.handleNSDelete(c, t.id)
	case http.MethodPatch:
		return h.handleNSPatch(c, t.id)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed.")
	}
}

// handleLegacyNamespace serves Network types the provider does not model through the generic store.
func (h *Handler) handleLegacyNamespace(c *echo.Context, segs []string, t nsTarget) error {
	if t.listing {
		return h.handleListResources(c, segs)
	}

	return h.handleGenericResource(c)
}

func (h *Handler) handleNSList(c *echo.Context, id ResourceID, segs []string) error {
	if c.Request().Method != http.MethodGet {
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed.")
	}

	if len(segs) == nsCheckIPSegs && strings.EqualFold(segs[nsCheckIPSegs-1], nsCheckIPAction) {
		return h.handleCheckIP(c, segs)
	}

	values, err := h.Registry.List(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, h.pageNS(c, values, nsDefaultTop(id.Namespace)))
}

const computePageSize = 100

// nsDefaultTop is the page size applied when the caller sends no $top; 0 means everything in one page.
func nsDefaultTop(ns string) int {
	if strings.EqualFold(ns, namespaceMicrosoftCompute) {
		return computePageSize
	}

	return 0
}

// pageNS applies $top/$skiptoken paging to a list result and adds nextLink when more items remain.
func (h *Handler) pageNS(c *echo.Context, values []map[string]any, defaultTop int) map[string]any {
	top, err := strconv.Atoi(c.QueryParam("$top"))
	if err != nil || top <= 0 {
		top = defaultTop
	}

	if top <= 0 {
		return map[string]any{fieldValue: values}
	}

	skip, _ := strconv.Atoi(c.QueryParam("$skiptoken"))
	skip = min(max(skip, 0), len(values))
	end := min(skip+top, len(values))
	out := map[string]any{fieldValue: values[skip:end]}

	if end < len(values) {
		q := url.Values{}
		q.Set(nsAPIVersionKey, c.QueryParam(nsAPIVersionKey))
		q.Set("$top", strconv.Itoa(top))
		q.Set("$skiptoken", strconv.Itoa(end))
		out["nextLink"] = h.baseURLFor(c.Request()) + c.Request().URL.Path + "?" + q.Encode()
	}

	return out
}

func (h *Handler) handleCheckIP(c *echo.Context, segs []string) error {
	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:len(segs)-1]))
	if err != nil || !strings.EqualFold(firstType(id), netVNets) || len(id.Types) != 1 {
		return h.writeNotFound(c)
	}

	np, ok := h.networkProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	out, err := np.CheckIPAddressAvailability(id, c.QueryParam("ipAddress"))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

// isSyncType reports whether writes to id complete without a long-running operation.
func isSyncType(id ResourceID) bool {
	if id.Namespace == namespaceMicrosoftManagedIdentity ||
		strings.EqualFold(id.Namespace, namespaceMicrosoftManagedIdentity) {
		return true
	}

	if strings.EqualFold(id.Namespace, namespaceMicrosoftCompute) && strings.EqualFold(firstType(id), cmpAvSets) {
		return true
	}

	if strings.EqualFold(id.Namespace, namespaceMicrosoftApp) && acaTargetOf(id).kind == acaKindStorage {
		return true
	}

	if strings.EqualFold(id.Namespace, namespaceMicrosoftContainerService) && aksTargetOf(id).kind == aksKindMaint {
		return true
	}

	if len(id.Types) == 2 && strings.EqualFold(id.Types[0], netZones) {
		return !strings.EqualFold(id.Types[1], netLinks)
	}

	return false
}

func (h *Handler) setNSOperation(c *echo.Context, id ResourceID, withLocation bool) {
	opID := uuid.NewString()
	base := h.baseURLFor(c.Request()) + "/subscriptions/" + id.SubscriptionID + "/providers/" + id.Namespace +
		"/locations/" + DefaultLocation + "/"
	query := "?api-version=" + url.QueryEscape(c.QueryParam(nsAPIVersionKey))

	hdr := c.Response().Header()
	hdr.Set("Azure-Asyncoperation", base+nsOperations+"/"+opID+query)

	if withLocation {
		hdr.Set("Location", base+nsOperationRes+"/"+opID+query)
	}
}

// preconditionFailed checks If-Match / If-None-Match against the stored etag and reports whether it answered 412.
func (h *Handler) preconditionFailed(c *echo.Context, id ResourceID) (bool, error) {
	ifNone := c.Request().Header.Get("If-None-Match")
	ifMatch := c.Request().Header.Get("If-Match")

	if ifNone == "" && ifMatch == "" {
		return false, nil
	}

	cur, err := h.Registry.Get(c.Request().Context(), id)
	exists := err == nil

	if et, ok := h.Registry.providerFor(id).(etagSource); ok {
		var tag string

		tag, exists = et.etagOf(id)
		cur = map[string]any{netFieldEtag: tag}
	}

	switch {
	case ifNone == "*" && exists:
		return true, h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailed",
			"The resource "+id.LeafName()+" already exists.")
	case ifMatch != "" && ifMatch != "*" && (!exists || !etagEqual(strOf(cur, netFieldEtag), ifMatch)):
		return true, h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailed",
			"The ETag value '"+ifMatch+"' does not match the current ETag of the resource.")
	}

	return false, nil
}

func etagEqual(stored, header string) bool {
	return stored == header || stored == strings.Trim(header, `"`)
}

func (h *Handler) handleNSPut(c *echo.Context, id ResourceID) error {
	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if done, perr := h.preconditionFailed(c, id); done {
		return perr
	}

	out, created, err := h.Registry.Put(c.Request().Context(), id, body)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	etag := strOf(out, netFieldEtag)
	if et, ok := h.Registry.providerFor(id).(etagSource); ok {
		etag, _ = et.etagOf(id)
	}

	if etag != "" {
		c.Response().Header().Set("ETag", etag)
	}

	if !isSyncType(id) {
		h.setNSOperation(c, id, false)
	}

	status := http.StatusOK
	if created && createAnswers201(id) {
		status = http.StatusCreated
	}

	return h.writeJSON(c, status, out)
}

func (h *Handler) handleNSDelete(c *echo.Context, id ResourceID) error {
	if done, perr := h.preconditionFailed(c, id); done {
		return perr
	}

	if err := h.Registry.Delete(c.Request().Context(), id); err != nil {
		if isNotFoundOnDelete(err) {
			return c.NoContent(http.StatusNoContent)
		}

		return h.writeAPIError(c, err)
	}

	if isSyncType(id) {
		return c.NoContent(http.StatusOK)
	}

	h.setNSOperation(c, id, true)

	return c.NoContent(http.StatusAccepted)
}

// tagPatcher is implemented by providers that support PATCH {tags}.
type tagPatcher interface {
	PatchTags(id ResourceID, tags map[string]string) (map[string]any, error)
}

// etagSource is implemented by providers whose resources carry an etag that is not part of the response body.
type etagSource interface {
	etagOf(id ResourceID) (string, bool)
}

// resourcePatcher is implemented by providers that support PATCH of more than tags.
type resourcePatcher interface {
	PatchResource(id ResourceID, body map[string]any) (map[string]any, error)
}

func (h *Handler) handleNSPatch(c *echo.Context, id ResourceID) error {
	p := h.Registry.providerFor(id)

	if rp, ok := p.(resourcePatcher); ok {
		body, err := decodeJSONBody(c.Request())
		if err != nil {
			return h.writeAPIError(c, err)
		}

		out, err := rp.PatchResource(id, body)
		if err != nil {
			return h.writeAPIError(c, err)
		}

		return h.writeJSON(c, http.StatusOK, out)
	}

	tp, ok := p.(tagPatcher)
	if !ok {
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowed", "Method not allowed.")
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out, err := tp.PatchTags(id, stringTags(body[fieldTags]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) networkProvider() (*NetworkProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftNetwork)
	if !ok {
		return nil, false
	}

	np, ok := p.(*NetworkProvider)

	return np, ok
}

func (h *Handler) managedIdentityProvider() (*ManagedIdentityProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftManagedIdentity)
	if !ok {
		return nil, false
	}

	mp, ok := p.(*ManagedIdentityProvider)

	return mp, ok
}

// createAnswers201 is false for the Compute types whose SDK operations accept only 200 or 202 on PUT.
func createAnswers201(id ResourceID) bool {
	if !strings.EqualFold(id.Namespace, namespaceMicrosoftCompute) {
		return true
	}

	switch canonicalCmpType(firstType(id)) {
	case cmpDisks, cmpSnaps, cmpAvSets:
		return false
	}

	return true
}
