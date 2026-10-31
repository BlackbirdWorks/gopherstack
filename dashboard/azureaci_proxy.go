package dashboard

import (
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureACIProxyPrefix  = "/dashboard/api/v1/azure/aci"
	azureACIMaxBodyBytes = 1 << 20

	aciPxSubList    = 5
	aciPxGroupList  = 7
	aciPxGroup      = 8
	aciPxAction     = 9
	aciPxLogs       = 11
	aciPxNamespace  = "microsoft.containerinstance"
	aciPxStart      = "start"
	aciPxStop       = "stop"
	aciPxRestart    = "restart"
	aciPxGroupsType = "containergroups"
)

var aciPxActions = []string{aciPxStart, aciPxStop, aciPxRestart} //nolint:gochecknoglobals // read-only lookup table

// registerAzureACIRoutes exposes Azure Container Instances container groups to the SPA by dispatching in-process to
// the ARM handler, so responses keep the real shape. Only container group reads, logs, start/stop/restart and delete
// are reachable.
func (h *DashboardHandler) registerAzureACIRoutes() {
	var inner *echo.Echo

	if h.config.AzureARMOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.AzureARMOps.Handler())
	}

	serve := func(c *echo.Context) error {
		if inner == nil {
			return azureACIFail(c, http.StatusServiceUnavailable, errServiceUnavailable,
				"Azure Container Instances is not enabled")
		}

		r := c.Request()

		armPath, ok := azureACIProxyPath(r.Method, strings.TrimPrefix(r.URL.EscapedPath(), azureACIProxyPrefix))
		if !ok {
			return azureACIFail(c, http.StatusBadRequest, "InvalidRequestUri",
				"unsupported Container Instances proxy request")
		}

		q := url.Values{}
		for _, k := range []string{armQueryVersion, "tail"} {
			if v := r.URL.Query().Get(k); v != "" {
				q.Set(k, v)
			}
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(armPath), armPath
		fwd.URL.RawQuery = q.Encode()
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, azureACIMaxBodyBytes)
		inner.ServeHTTP(c.Response(), fwd)

		return nil
	}

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		h.SubRouter.Add(method, azureACIProxyPrefix+"/*", serve)
	}
}

// azureACIProxyPath validates the proxied ARM path against the container group routes.
func azureACIProxyPath(method, raw string) (string, bool) {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.ContainsAny(dec, "/:") {
			return "", false
		}
	}

	if !aciProxyAllowed(method, strings.Split(strings.ToLower(strings.TrimPrefix(raw, "/")), "/")) {
		return "", false
	}

	return "/" + strings.Join(parts, "/"), true
}

type azureACIErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func azureACIFail(c *echo.Context, status int, code, msg string) error {
	return c.JSON(status, map[string]azureACIErrorBody{keyError: {Code: code, Message: msg}})
}

func aciProxyAllowed(method string, p []string) bool {
	n := len(p)
	if n < aciPxSubList || p[0] != "subscriptions" {
		return false
	}

	if n == aciPxSubList {
		return method == http.MethodGet && p[2] == "providers" && p[3] == aciPxNamespace && p[4] == aciPxGroupsType
	}

	if n < aciPxGroupList || p[2] != "resourcegroups" || p[4] != "providers" || p[5] != aciPxNamespace ||
		p[6] != aciPxGroupsType {
		return false
	}

	return aciProxyGroupOp(method, p)
}

func aciProxyGroupOp(method string, p []string) bool {
	n := len(p)

	switch method {
	case http.MethodGet:
		return n == aciPxGroupList || n == aciPxGroup || (n == aciPxLogs && p[8] == "containers" && p[10] == "logs")
	case http.MethodPost:
		return n == aciPxAction && slices.Contains(aciPxActions, p[8])
	case http.MethodDelete:
		return n == aciPxGroup
	}

	return false
}
