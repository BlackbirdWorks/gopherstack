package dashboard

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureTableProxyPrefix  = "/dashboard/api/v1/azure/table"
	azureTableMaxBodyBytes = 1 << 20
	azureTableSegments     = 2
	azureTableBatchSeg     = "$batch"
	azureTableCollection   = "Tables"
)

// registerAzureTableRoutes exposes the Azure Table emulator to the SPA by
// dispatching in-process to its handler, avoiding its separate listener and CORS.
func (h *DashboardHandler) registerAzureTableRoutes() {
	var handler echo.HandlerFunc

	if h.config.AzureTableOps != nil {
		handler = h.config.AzureTableOps.Handler()
	}

	serve := newAzureProxyHandler(azureTableProxyPrefix, azureTableMaxBodyBytes, handler, azureTableProxyPath)

	guarded := func(c *echo.Context) error {
		if !azureTableMethodOK(c.Request().Method, strings.TrimPrefix(c.Request().URL.Path, azureTableProxyPrefix)) {
			return c.String(http.StatusBadRequest, "unsupported proxy request")
		}

		return serve(c)
	}

	for _, method := range []string{
		http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
	} {
		h.SubRouter.Add(method, azureTableProxyPrefix+"/*", guarded)
	}
}

// azureTableMethodOK limits writes to what the Table service defines: POST on
// Tables or a table, PUT/PATCH/DELETE on a single entity, DELETE on Tables('x').
func azureTableMethodOK(method, raw string) bool {
	segs := strings.Split(strings.TrimPrefix(raw, "/"), "/")
	if len(segs) != azureTableSegments {
		return false
	}

	res := segs[1]
	isTableItem := strings.HasPrefix(res, azureTableCollection+"(")
	isEntity := !isTableItem && strings.HasSuffix(res, ")") && strings.Contains(res, "(")

	switch method {
	case http.MethodGet, http.MethodPost:
		return true
	case http.MethodDelete:
		return isTableItem || isEntity
	case http.MethodPut, http.MethodPatch:
		return isEntity
	default:
		return false
	}
}

// azureTableProxyPath admits only /<account>/<resource>, never $batch or
// anything with nested or dot segments.
func azureTableProxyPath(raw string) (string, bool) {
	if !strings.HasPrefix(raw, "/") || len(raw) < 2 {
		return "", false
	}

	segs := strings.Split(raw[1:], "/")
	if len(segs) != azureTableSegments {
		return "", false
	}

	for _, seg := range segs {
		if seg == "" || seg == ".." || seg == "." {
			return "", false
		}
	}

	if segs[1] == azureTableBatchSeg {
		return "", false
	}

	return raw, true
}
