package dashboard

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	ociCorePrefix    = ociProxyPrefix + "/core"
	ociCoreAPIPrefix = "/20160918"
	ociCoreMaxBody   = 1 << 16
	ociCoreItemSegs  = 2
)

var ociCoreReadKinds = []string{ //nolint:gochecknoglobals // immutable allowlist
	"vcns", "subnets", "routeTables", "securityLists", "networkSecurityGroups", "internetGateways",
	"natGateways", "serviceGateways", "dhcps", "localPeeringGateways", "publicIps", "privateIps",
	btInstancesKind, "vnicAttachments", "vnics", "volumes", "bootVolumes", "volumeAttachments",
	"bootVolumeAttachments", "volumeBackups", "shapes", "images",
}

var ociCoreInstanceActions = []string{ //nolint:gochecknoglobals // immutable
	"START", "STOP", "SOFTSTOP", "SOFTRESET", "RESET",
}

// registerOCICoreRoutes serves the Core Services page by dispatching in-process to the emulator's handler
// without request signing. Only the listed read paths and instance power actions are reachable.
func (h *DashboardHandler) registerOCICoreRoutes() {
	var (
		inner *echo.Echo
		comps func() []string
	)

	if ops := h.config.OCICoreOps; ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
		comps = ops.Backend.Compartments
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return ociGuard(inner != nil, "OCI Core Services", next)
	}

	h.SubRouter.Add(http.MethodGet, ociCorePrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()

		apiPath, ok := ociCoreProxyPath(r.Method, strings.TrimPrefix(r.URL.EscapedPath(), ociCorePrefix+"/api"),
			r.URL.Query())
		if !ok {
			return cloudJSONFail(
				c,
				http.StatusBadRequest,
				"InvalidParameter",
				"unsupported Core Services proxy request",
			)
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(apiPath), apiPath
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, ociCoreMaxBody)

		rec := httptest.NewRecorder()
		inner.ServeHTTP(rec, fwd)

		return ociCoreRelay(c, rec)
	})

	h.SubRouter.Add(http.MethodGet, ociCorePrefix+"/api/*", serve)
	h.SubRouter.Add(http.MethodPost, ociCorePrefix+"/api/*", serve)
}

// ociCoreRelay copies the emulator's response, masking values that must not reach the browser: iSCSI CHAP
// secrets and instance user data.
func ociCoreRelay(c *echo.Context, rec *httptest.ResponseRecorder) error {
	for _, k := range []string{"opc-next-page", "etag", "opc-request-id"} {
		if v := rec.Header().Get(k); v != "" {
			c.Response().Header().Set(k, v)
		}
	}

	body := rec.Body.Bytes()

	if rec.Code < http.StatusMultipleChoices && len(body) > 0 {
		var doc any
		if err := json.Unmarshal(body, &doc); err == nil {
			ociCoreScrub(doc)

			if out, merr := json.Marshal(doc); merr == nil {
				body = out
			}
		}
	}

	return c.JSONBlob(rec.Code, body)
}

func ociCoreScrub(v any) {
	switch t := v.(type) {
	case []any:
		for _, x := range t {
			ociCoreScrub(x)
		}
	case map[string]any:
		delete(t, "chapSecret")

		for _, key := range []string{"metadata", "extendedMetadata"} {
			if md, ok := t[key].(map[string]any); ok {
				if _, has := md["user_data"]; has {
					md["user_data"] = ociConfigMask
				}
			}
		}
	}
}

// ociCoreProxyPath maps a path below the proxy to the emulator's /20160918 path, enforcing the allowlist.
func ociCoreProxyPath(method, raw string, query url.Values) (string, bool) {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.Contains(dec, "/") {
			return "", false
		}
	}

	if !slices.Contains(ociCoreReadKinds, parts[0]) {
		return "", false
	}

	if method == http.MethodPost {
		if len(parts) != ociCoreItemSegs || parts[0] != btInstancesKind ||
			!slices.Contains(ociCoreInstanceActions, strings.ToUpper(query.Get("action"))) {
			return "", false
		}
	}

	return ociCoreAPIPrefix + "/" + strings.Join(parts, "/"), true
}
