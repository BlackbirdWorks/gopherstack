package azurearm

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	webSiteSegs       = 8
	webProviderNsIdx  = 5
	webTypeIdx        = 6
	webSubCheckLen    = 5
	webSubCheckAction = "checknameavailability"
	webRestartAction  = "restart"
	webPublishXML     = "publishxml"
	webSubKeysSeg     = "keys"
	webHostSeg        = "host"
	webFunctionsSeg   = "functions"
	webConfigSeg      = "config"
	webListSeg        = "list"
	webListKeysSeg    = "listkeys"
	webPoliciesSeg    = "basicpublishingcredentialspolicies"
)

func isWebSiteScope(segs []string) bool {
	return len(segs) >= webSiteSegs && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, webProviderNsIdx, namespaceMicrosoftWeb) && segAt(segs, webTypeIdx, webSitesType)
}

//nolint:gochecknoglobals // static route table, read-only after init
var webRoutes = []armRoute{
	{
		match: func(segs []string, r *http.Request) bool {
			return r.Method == http.MethodPost && len(segs) == webSubCheckLen && isSubscriptionScoped(segs) &&
				segAt(segs, webSubProviderSegIdx, providersSegment) &&
				segAt(segs, webSubProviderNsIdx, namespaceMicrosoftWeb) &&
				segAt(segs, webSubCheckActionIdx, webSubCheckAction)
		},
		handle: func(h *Handler, c *echo.Context, _ []string) error { return h.handleWebCheckName(c) },
	},
	{
		match: func(segs []string, r *http.Request) bool {
			return isWebSiteScope(segs) && !isSlotResource(segs, r.Method) &&
				(len(segs) > webSiteSegs || r.Method == http.MethodPatch)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleWebSite(c, segs) },
	},
}

// isSlotResource reports whether the request is plain GET/PUT/DELETE on sites/{name}/slots/{slot}, which the
// generic resource handler serves through the provider.
func isSlotResource(segs []string, method string) bool {
	if len(segs) < webSiteSegs+1 || !strings.EqualFold(segs[webSiteSegs], webSlotsType) {
		return false
	}

	return len(segs) == webSiteSegs+webSlotRestSegs &&
		(method == http.MethodGet || method == http.MethodPut || method == http.MethodDelete)
}

const (
	webSlotRestSegs      = 2
	webSubProviderSegIdx = 2
	webSubProviderNsIdx  = 3
	webSubCheckActionIdx = 4
)

func (h *Handler) webProviderOrNotFound(c *echo.Context) (*WebProvider, error) {
	p, ok := h.webProvider()
	if !ok {
		return nil, h.writeNotFound(c)
	}

	return p, nil
}

func (h *Handler) webProvider() (*WebProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftWeb)
	if !ok {
		return nil, false
	}

	wp, ok := p.(*WebProvider)

	return wp, ok
}

func (h *Handler) handleWebCheckName(c *echo.Context) error {
	p, err := h.webProviderOrNotFound(c)
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

// webSubRoute is one route below a site: pattern segments are matched case-insensitively, "*" matches any.
type webSubRoute struct {
	handle  func(h *Handler, c *echo.Context, p *WebProvider, id ResourceID, rest []string) error
	method  string
	pattern []string
}

func (r webSubRoute) matches(method string, rest []string) bool {
	if r.method != method || len(r.pattern) != len(rest) {
		return false
	}

	for i, want := range r.pattern {
		if want != "*" && !strings.EqualFold(want, rest[i]) {
			return false
		}
	}

	return true
}

func newWebSubRoute(
	method string, pattern []string,
	fn func(*Handler, *echo.Context, *WebProvider, ResourceID, []string) error,
) webSubRoute {
	return webSubRoute{handle: fn, method: method, pattern: pattern}
}

func webSubRoutes() []webSubRoute {
	post, get, put, del, patch := http.MethodPost, http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch

	return []webSubRoute{
		newWebSubRoute(post, []string{"start"}, webStateAction("start")),
		newWebSubRoute(post, []string{"stop"}, webStateAction("stop")),
		newWebSubRoute(post, []string{webRestartAction}, webStateAction(webRestartAction)),
		newWebSubRoute(post, []string{webPublishXML}, (*Handler).webPublishXML),
		newWebSubRoute(get, []string{webSlotsType}, (*Handler).webListSlots),
		newWebSubRoute(post, []string{"slotsswap"}, (*Handler).webSwapSlots),
		newWebSubRoute(post, []string{"applySlotConfig"}, (*Handler).webApplySlotConfig),
		newWebSubRoute(post, []string{"resetSlotConfig"}, (*Handler).webResetSlotConfig),
		newWebSubRoute(post, []string{"slotsdiffs"}, (*Handler).webSlotDiffs),
		newWebSubRoute(get, []string{webFunctionsSeg}, (*Handler).webListFunctions),
		newWebSubRoute(get, []string{webFunctionsSeg, "*"}, (*Handler).webGetFunction),
		newWebSubRoute(post, []string{webFunctionsSeg, "*", webListKeysSeg}, (*Handler).webFunctionKeys),
		newWebSubRoute(post, []string{webFunctionsSeg, "*", "listsecrets"}, (*Handler).webFunctionSecrets),
		newWebSubRoute(put, []string{webFunctionsSeg, "*", webSubKeysSeg, "*"}, (*Handler).webSetFunctionKey),
		newWebSubRoute(del, []string{webFunctionsSeg, "*", webSubKeysSeg, "*"}, (*Handler).webDeleteFunctionKey),
		newWebSubRoute(post, []string{webHostSeg, webKeyDefault, webListKeysSeg}, (*Handler).webHostKeys),
		newWebSubRoute(put, []string{webHostSeg, webKeyDefault, "*", "*"}, (*Handler).webSetHostKey),
		newWebSubRoute(del, []string{webHostSeg, webKeyDefault, "*", "*"}, (*Handler).webDeleteHostKey),
		newWebSubRoute(post, []string{webConfigSeg, webConfigPubCr, webListSeg}, (*Handler).webPublishingCredentials),
		newWebSubRoute(post, []string{webConfigSeg, "*", webListSeg}, (*Handler).webGetConfig),
		newWebSubRoute(get, []string{webConfigSeg, "*"}, (*Handler).webGetConfig),
		newWebSubRoute(put, []string{webConfigSeg, "*"}, webPutConfig(false)),
		newWebSubRoute(patch, []string{webConfigSeg, "*"}, webPutConfig(true)),
		newWebSubRoute(get, []string{webPoliciesSeg, "*"}, webPolicy(false)),
		newWebSubRoute(put, []string{webPoliciesSeg, "*"}, webPolicy(true)),
	}
}

// handleWebSite serves everything below (and PATCH on) a site; segs[:8] identify the site.
func (h *Handler) handleWebSite(c *echo.Context, segs []string) error {
	p, err := h.webProviderOrNotFound(c)
	if p == nil {
		return err
	}

	id, err := ParseGenericResourcePath("/" + joinSegs(segs[:webSiteSegs]))
	if err != nil {
		return h.writeAPIError(c, err)
	}

	rest := segs[webSiteSegs:]
	method := c.Request().Method

	if len(rest) >= webSlotRestSegs && strings.EqualFold(rest[0], webSlotsType) {
		id, err = ParseGenericResourcePath("/" + joinSegs(segs[:webSiteSegs+webSlotRestSegs]))
		if err != nil {
			return h.writeAPIError(c, err)
		}

		rest = rest[webSlotRestSegs:]
	}

	if len(rest) == 0 {
		return h.webPatch(c, p, id)
	}

	for _, route := range webSubRoutes() {
		if route.matches(method, rest) {
			return route.handle(h, c, p, id, rest)
		}
	}

	return h.writeNotFound(c)
}

func (h *Handler) webPatch(c *echo.Context, p *WebProvider, id ResourceID) error {
	return h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) {
		return p.Patch(c.Request().Context(), id, body)
	})
}

func webStateAction(action string) func(*Handler, *echo.Context, *WebProvider, ResourceID, []string) error {
	return func(h *Handler, c *echo.Context, p *WebProvider, id ResourceID, _ []string) error {
		if err := p.SetState(c.Request().Context(), id, action); err != nil {
			return h.writeAPIError(c, err)
		}

		return c.NoContent(http.StatusOK)
	}
}

func (h *Handler) webListSlots(c *echo.Context, p *WebProvider, id ResourceID, _ []string) error {
	slots, err := p.ListSlots(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: slots})
}

func (h *Handler) webSwapSlots(c *echo.Context, p *WebProvider, id ResourceID, _ []string) error {
	return h.writeSlotAction(c, func(body map[string]any) error { return p.SwapSlots(c.Request().Context(), id, body) })
}

func (h *Handler) webApplySlotConfig(c *echo.Context, p *WebProvider, id ResourceID, _ []string) error {
	return h.writeSlotAction(c, func(body map[string]any) error {
		return p.ApplySlotConfig(c.Request().Context(), id, body)
	})
}

func (h *Handler) webResetSlotConfig(c *echo.Context, p *WebProvider, id ResourceID, _ []string) error {
	if err := p.ResetSlotConfig(c.Request().Context(), id); err != nil {
		return h.writeAPIError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) webSlotDiffs(c *echo.Context, p *WebProvider, id ResourceID, _ []string) error {
	return h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) {
		diffs, err := p.SlotDifferences(c.Request().Context(), id, body)
		if err != nil {
			return nil, err
		}

		return map[string]any{fieldValue: diffs}, nil
	})
}

func (h *Handler) writeSlotAction(c *echo.Context, fn func(body map[string]any) error) error {
	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	if err = fn(body); err != nil {
		return h.writeAPIError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) webPublishXML(c *echo.Context, p *WebProvider, id ResourceID, _ []string) error {
	raw, err := p.PublishXML(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return c.Blob(http.StatusOK, "application/xml", raw)
}

func (h *Handler) webListFunctions(c *echo.Context, p *WebProvider, id ResourceID, _ []string) error {
	fns, err := p.ListFunctions(c.Request().Context(), id)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: fns})
}

func (h *Handler) webGetFunction(c *echo.Context, p *WebProvider, id ResourceID, rest []string) error {
	return h.writeResult(c, func() (map[string]any, error) { return p.GetFunction(c.Request().Context(), id, rest[1]) })
}

func (h *Handler) webFunctionKeys(c *echo.Context, p *WebProvider, id ResourceID, rest []string) error {
	return h.writeResult(
		c,
		func() (map[string]any, error) { return p.FunctionKeys(c.Request().Context(), id, rest[1]) },
	)
}

func (h *Handler) webFunctionSecrets(c *echo.Context, p *WebProvider, id ResourceID, rest []string) error {
	return h.writeResult(
		c,
		func() (map[string]any, error) { return p.FunctionSecrets(c.Request().Context(), id, rest[1]) },
	)
}

func (h *Handler) webSetFunctionKey(c *echo.Context, p *WebProvider, id ResourceID, rest []string) error {
	return h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) {
		return p.SetFunctionKey(c.Request().Context(), id, rest[1], rest[3], body)
	})
}

func (h *Handler) webDeleteFunctionKey(c *echo.Context, p *WebProvider, id ResourceID, rest []string) error {
	return h.writeNoContent(c, p.DeleteFunctionKey(c.Request().Context(), id, rest[1], rest[3]))
}

func (h *Handler) webHostKeys(c *echo.Context, p *WebProvider, id ResourceID, _ []string) error {
	return h.writeResult(c, func() (map[string]any, error) { return p.HostKeys(c.Request().Context(), id) })
}

func (h *Handler) webSetHostKey(c *echo.Context, p *WebProvider, id ResourceID, rest []string) error {
	return h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) {
		return p.SetHostKey(c.Request().Context(), id, rest[2], rest[3], body)
	})
}

func (h *Handler) webDeleteHostKey(c *echo.Context, p *WebProvider, id ResourceID, rest []string) error {
	return h.writeNoContent(c, p.DeleteHostKey(c.Request().Context(), id, rest[2], rest[3]))
}

func (h *Handler) webPublishingCredentials(c *echo.Context, p *WebProvider, id ResourceID, _ []string) error {
	return h.writeResult(
		c,
		func() (map[string]any, error) { return p.PublishingCredentials(c.Request().Context(), id) },
	)
}

func (h *Handler) webGetConfig(c *echo.Context, p *WebProvider, id ResourceID, rest []string) error {
	return h.writeResult(c, func() (map[string]any, error) { return p.GetConfig(c.Request().Context(), id, rest[1]) })
}

func webPutConfig(merge bool) func(*Handler, *echo.Context, *WebProvider, ResourceID, []string) error {
	return func(h *Handler, c *echo.Context, p *WebProvider, id ResourceID, rest []string) error {
		return h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) {
			return p.PutConfig(c.Request().Context(), id, rest[1], body, merge)
		})
	}
}

func webPolicy(put bool) func(*Handler, *echo.Context, *WebProvider, ResourceID, []string) error {
	return func(h *Handler, c *echo.Context, p *WebProvider, id ResourceID, rest []string) error {
		return h.writeBodyOptional(c, put, func(body map[string]any) (map[string]any, error) {
			return p.Policy(c.Request().Context(), id, rest[1], body, put)
		})
	}
}

func (h *Handler) writeNoContent(c *echo.Context, err error) error {
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) writeResult(c *echo.Context, fn func() (map[string]any, error)) error {
	out, err := fn()
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, out)
}

func (h *Handler) writeBodyResult(c *echo.Context, fn func(body map[string]any) (map[string]any, error)) error {
	return h.writeBodyOptional(c, true, fn)
}

func (h *Handler) writeBodyOptional(
	c *echo.Context, read bool, fn func(body map[string]any) (map[string]any, error),
) error {
	var body map[string]any

	if read {
		var err error
		if body, err = decodeJSONBody(c.Request()); err != nil {
			return h.writeAPIError(c, err)
		}
	}

	return h.writeResult(c, func() (map[string]any, error) { return fn(body) })
}
