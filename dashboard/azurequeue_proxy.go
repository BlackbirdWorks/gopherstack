package dashboard

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureQueueProxyPrefix  = "/dashboard/api/v1/azure/queue"
	azureQueueMaxBodyBytes = 1 << 20
	azureQueueMaxSegments  = 4
	azureQueueMessagesSeg  = "messages"
)

// registerAzureQueueRoutes exposes the Azure Queue emulator to the SPA by
// dispatching in-process to its handler, avoiding its separate listener and CORS.
func (h *DashboardHandler) registerAzureQueueRoutes() {
	var handler echo.HandlerFunc

	if h.config.AzureQueueOps != nil {
		handler = h.config.AzureQueueOps.Handler()
	}

	serve := newAzureProxyHandler(azureQueueProxyPrefix, azureQueueMaxBodyBytes, handler, azureQueueProxyPath)

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodPost, http.MethodDelete} {
		h.SubRouter.Add(method, azureQueueProxyPrefix+"/*", serve)
	}
}

// newAzureProxyHandler dispatches /<prefix>/<path> in-process to inner after
// pathOK vets the path; a nil inner answers 503.
func newAzureProxyHandler(
	prefix string, maxBody int64, inner echo.HandlerFunc, pathOK func(string) (string, bool),
) echo.HandlerFunc {
	var router *echo.Echo

	if inner != nil {
		router = echo.New()
		router.Any("/*", inner)
	}

	return func(c *echo.Context) error {
		if router == nil {
			return c.XML(
				http.StatusServiceUnavailable,
				map[string]string{"Code": http.StatusText(http.StatusServiceUnavailable)},
			)
		}

		r := c.Request()

		p, ok := pathOK(strings.TrimPrefix(r.URL.Path, prefix))
		if !ok {
			return c.String(http.StatusBadRequest, "unsupported proxy request")
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path = p
		fwd.URL.RawPath = ""
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, maxBody)
		router.ServeHTTP(c.Response(), fwd)

		return nil
	}
}

// azureQueueProxyPath admits only /<account>[/<queue>[/messages[/<id>]]].
func azureQueueProxyPath(raw string) (string, bool) {
	if !strings.HasPrefix(raw, "/") || len(raw) < 2 {
		return "", false
	}

	segs := strings.Split(raw[1:], "/")
	if len(segs) > azureQueueMaxSegments {
		return "", false
	}

	for _, seg := range segs {
		if seg == "" || seg == ".." || seg == "." {
			return "", false
		}
	}

	if len(segs) > 2 && segs[2] != azureQueueMessagesSeg {
		return "", false
	}

	return raw, true
}
