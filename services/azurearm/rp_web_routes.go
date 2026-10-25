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
	webMinConfigPath  = 2
	webHostPathLen    = 3
	webFnKeyPathLen   = 4
	webFnActionLen    = 3
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
			return isWebSiteScope(segs) && (len(segs) > webSiteSegs || r.Method == http.MethodPatch)
		},
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleWebSite(c, segs) },
	},
}

const (
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

	if len(rest) == 0 {
		return h.webPatch(c, p, id)
	}

	for _, route := range []func(*Handler, *echo.Context, *WebProvider, ResourceID, []string, string) (bool, error){
		(*Handler).webSiteActions, (*Handler).webFunctions, (*Handler).webHost, (*Handler).webConfig,
	} {
		if handled, routeErr := route(h, c, p, id, rest, method); handled {
			return routeErr
		}
	}

	return h.writeNotFound(c)
}

func (h *Handler) webPatch(c *echo.Context, p *WebProvider, id ResourceID) error {
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

func (h *Handler) webSiteActions(
	c *echo.Context, p *WebProvider, id ResourceID, rest []string, method string,
) (bool, error) {
	if len(rest) != 1 {
		return false, nil
	}

	action := strings.ToLower(rest[0])

	switch {
	case method == http.MethodPost && (action == "start" || action == "stop" || action == webRestartAction):
		if err := p.SetState(c.Request().Context(), id, action); err != nil {
			return true, h.writeAPIError(c, err)
		}

		return true, c.NoContent(http.StatusOK)
	case method == http.MethodPost && action == webPublishXML:
		raw, err := p.PublishXML(c.Request().Context(), id)
		if err != nil {
			return true, h.writeAPIError(c, err)
		}

		return true, c.Blob(http.StatusOK, "application/xml", raw)
	}

	return false, nil
}

func (h *Handler) webFunctions(
	c *echo.Context, p *WebProvider, id ResourceID, rest []string, method string,
) (bool, error) {
	if !strings.EqualFold(rest[0], webFunctionsSeg) {
		return false, nil
	}

	ctx := c.Request().Context()

	switch {
	case len(rest) == 1 && method == http.MethodGet:
		fns, err := p.ListFunctions(ctx, id)
		if err != nil {
			return true, h.writeAPIError(c, err)
		}

		return true, h.writeJSON(c, http.StatusOK, map[string]any{fieldValue: fns})
	case len(rest) == 2 && method == http.MethodGet:
		return true, h.writeResult(c, func() (map[string]any, error) { return p.GetFunction(ctx, id, rest[1]) })
	case len(rest) == webFnActionLen && method == http.MethodPost && strings.EqualFold(rest[2], webListKeysSeg):
		return true, h.writeResult(c, func() (map[string]any, error) { return p.FunctionKeys(ctx, id, rest[1]) })
	case len(rest) == webFnActionLen && method == http.MethodPost && strings.EqualFold(rest[2], "listsecrets"):
		return true, h.writeResult(c, func() (map[string]any, error) { return p.FunctionSecrets(ctx, id, rest[1]) })
	case len(rest) == webFnKeyPathLen && strings.EqualFold(rest[2], webSubKeysSeg) && method == http.MethodPut:
		return true, h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) {
			return p.SetFunctionKey(ctx, id, rest[1], rest[3], body)
		})
	case len(rest) == webFnKeyPathLen && strings.EqualFold(rest[2], webSubKeysSeg) && method == http.MethodDelete:
		if err := p.DeleteFunctionKey(ctx, id, rest[1], rest[3]); err != nil {
			return true, h.writeAPIError(c, err)
		}

		return true, c.NoContent(http.StatusNoContent)
	}

	return false, nil
}

func (h *Handler) webHost(
	c *echo.Context, p *WebProvider, id ResourceID, rest []string, method string,
) (bool, error) {
	if !strings.EqualFold(rest[0], webHostSeg) || len(rest) < webHostPathLen || !strings.EqualFold(rest[1], "default") {
		return false, nil
	}

	ctx := c.Request().Context()

	switch {
	case len(rest) == webHostPathLen && method == http.MethodPost && strings.EqualFold(rest[2], webListKeysSeg):
		return true, h.writeResult(c, func() (map[string]any, error) { return p.HostKeys(ctx, id) })
	case len(rest) == webFnKeyPathLen && method == http.MethodPut:
		return true, h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) {
			return p.SetHostKey(ctx, id, rest[2], rest[3], body)
		})
	case len(rest) == webFnKeyPathLen && method == http.MethodDelete:
		if err := p.DeleteHostKey(ctx, id, rest[2], rest[3]); err != nil {
			return true, h.writeAPIError(c, err)
		}

		return true, c.NoContent(http.StatusNoContent)
	}

	return false, nil
}

func (h *Handler) webConfig(
	c *echo.Context, p *WebProvider, id ResourceID, rest []string, method string,
) (bool, error) {
	ctx := c.Request().Context()

	switch {
	case strings.EqualFold(rest[0], webPoliciesSeg) && len(rest) == webMinConfigPath:
		put := method == http.MethodPut
		if !put && method != http.MethodGet {
			return false, nil
		}

		return true, h.writeBodyOptional(c, put, func(body map[string]any) (map[string]any, error) {
			return p.Policy(ctx, id, rest[1], body, put)
		})
	case !strings.EqualFold(rest[0], webConfigSeg) || len(rest) < webMinConfigPath:
		return false, nil
	case len(rest) == webHostPathLen && strings.EqualFold(rest[1], webConfigPubCr) &&
		strings.EqualFold(rest[2], webListSeg) && method == http.MethodPost:
		return true, h.writeResult(c, func() (map[string]any, error) { return p.PublishingCredentials(ctx, id) })
	case len(rest) == webHostPathLen && strings.EqualFold(rest[2], webListSeg) && method == http.MethodPost:
		return true, h.writeResult(c, func() (map[string]any, error) { return p.GetConfig(ctx, id, rest[1]) })
	case len(rest) == webMinConfigPath && method == http.MethodGet:
		return true, h.writeResult(c, func() (map[string]any, error) { return p.GetConfig(ctx, id, rest[1]) })
	case len(rest) == webMinConfigPath && (method == http.MethodPut || method == http.MethodPatch):
		merge := method == http.MethodPatch

		return true, h.writeBodyResult(c, func(body map[string]any) (map[string]any, error) {
			return p.PutConfig(ctx, id, rest[1], body, merge)
		})
	}

	return false, nil
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
