package azurearm

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

const (
	apimNsIdx    = 5
	apimTypeIdx  = 6
	apimSvcIdx   = 7
	apimMinSegs  = 8
	apimCheckLen = 5
)

func isAPIMScope(segs []string) bool {
	return len(segs) >= apimMinSegs && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, resourceGroupsSegment) &&
		segAt(segs, providerNamespaceActionIdx, providersSegment) &&
		segAt(segs, apimNsIdx, namespaceMicrosoftAPIM) && segAt(segs, apimTypeIdx, apimServiceType)
}

func isAPIMCheckName(segs []string, r *http.Request) bool {
	return r.Method == http.MethodPost && len(segs) == apimCheckLen && isSubscriptionScoped(segs) &&
		segAt(segs, providersOrResourceGroupsIdx, providersSegment) &&
		segAt(segs, providerNamespaceActionIdx-1, namespaceMicrosoftAPIM) &&
		segAt(segs, apimCheckLen-1, acrActionCheckName)
}

//nolint:gochecknoglobals // static route table, read-only after init
var apimRoutes = []armRoute{
	{
		match:  func(segs []string, _ *http.Request) bool { return isAPIMScope(segs) },
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAPIM(c, segs) },
	},
	{
		match:  isAPIMCheckName,
		handle: func(h *Handler, c *echo.Context, segs []string) error { return h.handleAPIMCheckName(c, segs) },
	},
}

func (h *Handler) apimProvider() (*APIMProvider, bool) {
	p, ok := h.Registry.ProviderNamed(namespaceMicrosoftAPIM)
	if !ok {
		return nil, false
	}

	ap, ok := p.(*APIMProvider)

	return ap, ok
}

func (h *Handler) handleAPIM(c *echo.Context, segs []string) error {
	p, ok := h.apimProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	r := c.Request()
	req := apimReq{
		Method: r.Method, Sub: segs[1], RG: segs[3], Svc: segs[apimSvcIdx], Rest: segs[apimMinSegs:],
		Query: r.URL.Query(), IfMatch: r.Header.Get("If-Match"),
	}

	if r.Method == http.MethodPut || r.Method == http.MethodPatch {
		body, err := decodeJSONBody(r)
		if err != nil {
			return h.writeAPIError(c, err)
		}

		req.Body = body
	}

	resp, err := p.Do(req)
	if err != nil {
		return h.writeAPIError(c, err)
	}

	hdr := c.Response().Header()
	if resp.Etag != "" {
		hdr.Set("ETag", resp.Etag)
	}

	if resp.OpID != "" {
		h.setAPIMOperation(c, segs, resp)
	}

	if resp.Retry {
		hdr.Set(apimCRetryAfter, "1")
	}

	if resp.Body == nil || r.Method == http.MethodHead {
		return c.NoContent(resp.Status)
	}

	return h.writeJSON(c, resp.Status, resp.Body)
}

func (h *Handler) handleAPIMCheckName(c *echo.Context, segs []string) error {
	p, ok := h.apimProvider()
	if !ok {
		return h.writeNotFound(c)
	}

	body, err := decodeJSONBody(c.Request())
	if err != nil {
		return h.writeAPIError(c, err)
	}

	return h.writeJSON(c, http.StatusOK, p.CheckName(segs[1], strOf(body, fieldName)))
}
