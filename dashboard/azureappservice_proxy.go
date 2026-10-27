package dashboard

import (
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureAppServiceProxyPrefix  = "/dashboard/api/v1/azure/appservice"
	azureAppServiceMaxBodyBytes = 1 << 20

	appPxSubList   = 5
	appPxSite      = 8
	appPxNamespace = "microsoft.web"
	appPxSitesType = "sites"
	appPxSlots     = "slots"
	appPxSlotItem  = 2
	appPxConfig    = "config"
	appPxSticky    = "slotconfignames"
	appPxConfigLen = 2
)

var appPxActions = []string{ //nolint:gochecknoglobals // read-only lookup table
	aciPxStart, aciPxStop, aciPxRestart, "slotsswap", "applyslotconfig", "resetslotconfig", "slotsdiffs",
}

// registerAzureAppServiceRoutes exposes Microsoft.Web web apps and their slots to the SPA by dispatching
// in-process to the ARM handler, so responses keep the real shape. Only site and slot reads, the sticky-setting
// names, lifecycle actions and the swap actions are reachable; application settings never pass through.
func (h *DashboardHandler) registerAzureAppServiceRoutes() {
	var inner *echo.Echo

	if h.config.AzureARMOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.AzureARMOps.Handler())
	}

	serve := func(c *echo.Context) error {
		if inner == nil {
			return azureACIFail(c, http.StatusServiceUnavailable, errServiceUnavailable,
				"Azure App Service is not enabled")
		}

		r := c.Request()

		armPath, ok := azureAppServiceProxyPath(r.Method,
			strings.TrimPrefix(r.URL.EscapedPath(), azureAppServiceProxyPrefix))
		if !ok {
			return azureACIFail(c, http.StatusBadRequest, "InvalidRequestUri",
				"unsupported App Service proxy request")
		}

		q := url.Values{}
		if v := r.URL.Query().Get("api-version"); v != "" {
			q.Set("api-version", v)
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(armPath), armPath
		fwd.URL.RawQuery = q.Encode()
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, azureAppServiceMaxBodyBytes)
		inner.ServeHTTP(c.Response(), fwd)

		return nil
	}

	for _, method := range []string{http.MethodGet, http.MethodPost} {
		h.SubRouter.Add(method, azureAppServiceProxyPrefix+"/*", serve)
	}
}

// azureAppServiceProxyPath validates the proxied ARM path against the Microsoft.Web site routes.
func azureAppServiceProxyPath(method, raw string) (string, bool) {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.ContainsAny(dec, "/:") {
			return "", false
		}
	}

	if !appProxyAllowed(method, strings.Split(strings.ToLower(strings.TrimPrefix(raw, "/")), "/")) {
		return "", false
	}

	return "/" + strings.Join(parts, "/"), true
}

func appProxyAllowed(method string, p []string) bool {
	n := len(p)
	if n < appPxSubList || p[0] != azureServiceBusSubs {
		return false
	}

	if n == appPxSubList {
		return method == http.MethodGet && p[2] == earcProvidersKind && p[3] == appPxNamespace &&
			p[4] == appPxSitesType
	}

	if !appProxyIsSite(p) {
		return false
	}

	rest := p[appPxSite:]
	if len(rest) >= appPxSlotItem && rest[0] == appPxSlots {
		return appProxySlotOp(method, rest[appPxSlotItem:])
	}

	if len(rest) == 1 && rest[0] == appPxSlots {
		return method == http.MethodGet
	}

	return appProxySiteOp(method, rest)
}

func appProxyIsSite(p []string) bool {
	return len(p) >= appPxSite && p[2] == "resourcegroups" && p[4] == earcProvidersKind &&
		p[5] == appPxNamespace && p[6] == appPxSitesType
}

// appProxySlotOp allows the operations below sites/{name}/slots/{slot}; rest is empty for the slot itself.
func appProxySlotOp(method string, rest []string) bool {
	if len(rest) == 0 {
		return method == http.MethodGet
	}

	return appProxySiteOp(method, rest)
}

func appProxySiteOp(method string, rest []string) bool {
	switch method {
	case http.MethodGet:
		return len(rest) == 0 || (len(rest) == appPxConfigLen && rest[0] == appPxConfig && rest[1] == appPxSticky)
	case http.MethodPost:
		return len(rest) == 1 && slices.Contains(appPxActions, rest[0])
	}

	return false
}
