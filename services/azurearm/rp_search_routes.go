package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	srchActAdminKeys   = "listAdminKeys"
	srchActRegenerate  = "regenerateAdminKey"
	srchActCreateQuery = "createQueryKey"
	srchActListQuery   = "listQueryKeys"
	srchActDeleteQuery = "deleteQueryKey"
	srchActCheckName   = "checkNameAvailability"
	srchActUsages      = "usages"
	srchProvidersIdx   = 2
	srchServiceLen     = 8
	srchActionIdx      = 8
	srchSubLen         = 5
	srchUsagesLen      = 7
	srchLocationIdx    = 5
	srchNsIdx          = 3
	srchServiceNsIdx   = 5
	srchServiceTypeIdx = 6
	srchUsageSKUIdx    = 7
)

func isSearchSub(segs []string, n int) bool {
	return len(segs) == n && isSubscriptionScoped(segs) &&
		segAt(segs, srchProvidersIdx, providersSegment) &&
		segAt(segs, srchNsIdx, namespaceMicrosoftSearch)
}

func isSearchService(segs []string) bool {
	return len(segs) >= srchServiceLen && isSubscriptionScoped(segs) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, srchServiceNsIdx, namespaceMicrosoftSearch) && segAt(segs, srchServiceTypeIdx, srchServicesType)
}

func isSearchAction(segs []string, r *http.Request, method, action string, withArg bool) bool {
	want := srchActionIdx + 1
	if withArg {
		want++
	}

	return r.Method == method && len(segs) == want && isSearchService(segs) &&
		strings.EqualFold(segs[srchActionIdx], action)
}

//nolint:gochecknoglobals // static route table, read-only after init
var searchRoutes = []armRoute{
	{
		match:  isSearchOperations,
		handle: func(h *Handler, c *echo.Context, _ []string) error { return h.handleSearchOperations(c) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isSearchLinks(segs, r, http.MethodGet, srchLinkListLen)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSearchLinkList(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isSearchLinks(segs, r, http.MethodGet, srchLinkItemLen)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSearchLinkGet(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isSearchLinks(segs, r, http.MethodPut, srchLinkItemLen)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSearchLinkPut(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isSearchLinks(segs, r, http.MethodDelete, srchLinkItemLen)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSearchLinkDelete(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPost && isSearchSub(segs, srchSubLen) &&
				segAt(segs, srchSubLen-1, srchActCheckName)
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error { return h.handleSearchCheckName(c) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodGet &&
				(isSearchSub(segs, srchUsagesLen) || isSearchSub(segs, srchUsagesLen+1)) &&
				segAt(segs, srchLocationIdx-1, "locations") &&
				segAt(segs, srchUsagesLen-1, srchActUsages)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSearchUsages(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPatch && len(segs) == srchServiceLen && isSearchService(segs)
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error { return h.handleSearchPatch(c) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isSearchAction(segs, r, http.MethodPost, srchActAdminKeys, false)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSearchAdminKeys(c, segs, "") },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isSearchAction(segs, r, http.MethodPost, srchActRegenerate, true)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error {
			return h.handleSearchAdminKeys(c, segs, segs[len(segs)-1])
		},
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isSearchAction(segs, r, http.MethodPost, srchActCreateQuery, true)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSearchCreateQueryKey(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isSearchAction(segs, r, http.MethodGet, srchActListQuery, false) ||
				isSearchAction(segs, r, http.MethodPost, srchActListQuery, false)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSearchListQueryKeys(c, segs) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isSearchAction(segs, r, http.MethodDelete, srchActDeleteQuery, true)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleSearchDeleteQueryKey(c, segs) },
	},
}

func (h *Handler) searchProvider() (*SearchProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftSearch)
	if !ok {
		return nil, false
	}

	sp, ok := p.(*SearchProvider)

	return sp, ok
}

func (h *Handler) searchOrNotFound(c *echo.Context, segs []string) (*SearchProvider, ResourceID, error) {
	p, ok := h.searchProvider()
	if !ok {
		return nil, ResourceID{}, h.writeNotFound(c)
	}

	if segs == nil {
		return p, ResourceID{}, nil
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:srchServiceLen]))
	if err != nil {
		return nil, ResourceID{}, h.writeAPIError(c, err)
	}

	return p, id, nil
}

func (h *Handler) handleSearchCheckName(c *echo.Context) error {
	p, _, err := h.searchOrNotFound(c, nil)
	if p == nil {
		return err
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	name, _ := body[fieldName].(string)

	return h.writeJSON(c, http.StatusOK, p.CheckName(name))
}

func (h *Handler) handleSearchUsages(c *echo.Context, segs []string) error {
	p, _, err := h.searchOrNotFound(c, nil)
	if p == nil {
		return err
	}

	sku := ""
	if len(segs) > srchUsageSKUIdx {
		sku = segs[srchUsageSKUIdx]
	}

	out, uErr := p.Usages(segs[1], segs[srchLocationIdx], sku)
	if uErr != nil {
		return h.writeAPIError(c, uErr)
	}

	if sku != "" {
		return h.writeJSON(c, http.StatusOK, out[0])
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: out})
}

func (h *Handler) handleSearchPatch(c *echo.Context) error {
	p, id, err := h.searchOrNotFound(c, splitARMPath(c.Request().URL.Path))
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

func (h *Handler) handleSearchAdminKeys(c *echo.Context, segs []string, kind string) error {
	p, id, err := h.searchOrNotFound(c, segs)
	if p == nil {
		return err
	}

	keys, err := p.AdminKeys(id, kind)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{srchKeyPrimary: keys.Primary, srchKeySecondary: keys.Secondary})
}

func (h *Handler) handleSearchCreateQueryKey(c *echo.Context, segs []string) error {
	p, id, err := h.searchOrNotFound(c, segs)
	if p == nil {
		return err
	}

	key, err := p.CreateQueryKey(id, segs[len(segs)-1])
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldName: key.Name, srchFieldKey: key.Key})
}

func (h *Handler) handleSearchListQueryKeys(c *echo.Context, segs []string) error {
	p, id, err := h.searchOrNotFound(c, segs)
	if p == nil {
		return err
	}

	keys, err := p.ListQueryKeys(id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{fieldName: k.Name, srchFieldKey: k.Key})
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: out})
}

func (h *Handler) handleSearchDeleteQueryKey(c *echo.Context, segs []string) error {
	p, id, err := h.searchOrNotFound(c, segs)
	if p == nil {
		return err
	}

	if err = p.DeleteQueryKey(id, segs[len(segs)-1]); err != nil {
		return h.writeAPIError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

const (
	srchKeyPrimary   = "primaryKey"
	srchKeySecondary = "secondaryKey"
	srchFieldKey     = "key"
)
