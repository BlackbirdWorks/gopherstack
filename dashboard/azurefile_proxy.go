package dashboard

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureFileProxyPrefix   = "/dashboard/api/v1/azure/file"
	azureFileAccountsRoute = "/dashboard/api/v1/azure/fileaccounts"
	azureFileMaxBodyBytes  = 1 << 20
	azureFileProxyHost     = "localhost"
)

// registerAzureFileRoutes exposes the Azure Files emulator to the SPA by dispatching read and delete requests
// in-process to its handler, which avoids its separate listener and CORS.
func (h *DashboardHandler) registerAzureFileRoutes() {
	var inner *echo.Echo

	if h.config.AzureFileOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.AzureFileOps.Handler())

		h.SubRouter.Add(http.MethodGet, azureFileAccountsRoute, func(c *echo.Context) error {
			return c.JSON(http.StatusOK, map[string]any{"accounts": h.config.AzureFileOps.Backend.Accounts()})
		})
	} else {
		h.SubRouter.Add(http.MethodGet, azureFileAccountsRoute, func(c *echo.Context) error {
			return cloudUnavailable(c, "Azure Files")
		})
	}

	serve := func(c *echo.Context) error {
		if inner == nil {
			return c.XMLBlob(http.StatusServiceUnavailable, []byte(
				`<?xml version="1.0" encoding="UTF-8"?><Error><Code>`+errServiceUnavailable+`</Code></Error>`))
		}

		r := c.Request()

		filePath, ok := azureFileProxyPath(strings.TrimPrefix(r.URL.EscapedPath(), azureFileProxyPrefix))
		if !ok {
			return c.String(http.StatusBadRequest, "unsupported file proxy request")
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(filePath), filePath
		fwd.Host = azureFileProxyHost
		fwd.RequestURI = ""
		fwd.Header.Del("Authorization")
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, azureFileMaxBodyBytes)
		inner.ServeHTTP(c.Response(), fwd)

		return nil
	}

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodDelete} {
		h.SubRouter.Add(method, azureFileProxyPrefix+"/*", serve)
	}
}

// azureFileProxyPath requires an /<account>[/...] path and rejects dot and empty-escape segments.
func azureFileProxyPath(raw string) (string, bool) {
	if !strings.HasPrefix(raw, "/") || len(raw) < 2 {
		return "", false
	}

	for seg := range strings.SplitSeq(raw[1:], "/") {
		if dec := unescapedPath(seg); dec == ".." || dec == "." || strings.Contains(dec, "/") {
			return "", false
		}
	}

	return raw, true
}
