package dashboard

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureBlobProxyPrefix  = "/dashboard/api/v1/azure/blob"
	azureBlobMaxBodyBytes = 64 << 20
)

// registerAzureBlobRoutes exposes the Azure Blob emulator to the SPA by
// dispatching in-process to its handler, avoiding its separate listener and CORS.
func (h *DashboardHandler) registerAzureBlobRoutes() {
	var inner *echo.Echo

	if h.config.AzureBlobOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.AzureBlobOps.Handler())
	}

	serve := func(c *echo.Context) error {
		if inner == nil {
			return c.XML(http.StatusServiceUnavailable, map[string]string{"Code": "ServiceUnavailable"})
		}

		r := c.Request()

		blobPath, ok := azureBlobProxyPath(strings.TrimPrefix(r.URL.Path, azureBlobProxyPrefix))
		if !ok {
			return c.String(http.StatusBadRequest, "unsupported blob proxy request")
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path = blobPath
		fwd.URL.RawPath = ""
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, azureBlobMaxBodyBytes)
		inner.ServeHTTP(c.Response(), fwd)

		return nil
	}

	for _, method := range []string{http.MethodGet, http.MethodHead, http.MethodPut, http.MethodDelete} {
		h.SubRouter.Add(method, azureBlobProxyPrefix+"/*", serve)
	}
}

// azureBlobProxyPath requires an /<account>[/...] path and rejects dot segments.
func azureBlobProxyPath(raw string) (string, bool) {
	if !strings.HasPrefix(raw, "/") || len(raw) < 2 {
		return "", false
	}

	for seg := range strings.SplitSeq(raw, "/") {
		if seg == ".." || seg == "." {
			return "", false
		}
	}

	return raw, true
}
