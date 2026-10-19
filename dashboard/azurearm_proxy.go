package dashboard

import (
	"net/http"
	"path"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureARMProxyPrefix  = "/dashboard/api/v1/azure/arm"
	azureARMMaxBodyBytes = 1 << 20
)

// registerAzureARMRoutes exposes the Azure Resource Manager emulator to the SPA by
// dispatching in-process to its handler, avoiding its self-signed HTTPS listener.
func (h *DashboardHandler) registerAzureARMRoutes() {
	var inner *echo.Echo

	if h.config.AzureARMOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.AzureARMOps.Handler())
	}

	serve := func(c *echo.Context) error {
		if inner == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]any{
				keyError: map[string]string{"code": errServiceUnavailable, "message": "Azure ARM is not enabled"},
			})
		}

		r := c.Request()

		armPath, ok := azureARMProxyPath(r.Method, strings.TrimPrefix(r.URL.Path, azureARMProxyPrefix))
		if !ok {
			return c.JSON(http.StatusBadRequest, map[string]any{
				"error": map[string]string{"code": "InvalidRequestUri", "message": "unsupported ARM proxy request"},
			})
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path = armPath
		fwd.URL.RawPath = ""
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, azureARMMaxBodyBytes)
		inner.ServeHTTP(c.Response(), fwd)

		return nil
	}

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPost} {
		h.SubRouter.Add(method, azureARMProxyPrefix+"/*", serve)
	}
}

// azureARMProxyPath validates and normalises the proxied ARM path. Only the
// /subscriptions and /tenants trees are reachable; POST is limited to listKeys.
func azureARMProxyPath(method, raw string) (string, bool) {
	if strings.Contains(raw, "..") {
		return "", false
	}

	clean := path.Clean("/" + raw)
	root := strings.ToLower(strings.Split(strings.TrimPrefix(clean, "/"), "/")[0])

	if root != "subscriptions" && root != "tenants" {
		return "", false
	}

	switch method {
	case http.MethodGet, http.MethodPut, http.MethodDelete:
		return clean, true
	case http.MethodPost:
		return clean, strings.HasSuffix(strings.ToLower(clean), "/listkeys")
	default:
		return "", false
	}
}
