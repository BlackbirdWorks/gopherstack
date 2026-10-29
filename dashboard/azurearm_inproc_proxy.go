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
	armPxMaxBody   = 1 << 20
	armPxSubList   = 5
	armPxItem      = 8
	armPxNSSub     = 3
	armPxTypeSub   = 4
	armPxNSRG      = 5
	armPxTypeRG    = 6
	armPxSubsSeg   = "subscriptions"
	armPxRGsSeg    = "resourcegroups"
	armPxProvSeg   = "providers"
	armPxSecretsKy = "secrets"
)

var armPxSecretKeys = []string{ //nolint:gochecknoglobals // immutable allowlist
	"adminpassword", "customdata", "sharedkey", "daprainstrumentationkey", "dapraiconnectionstring",
	"primarykey", "secondarykey", "protectedsettings",
}

// armProxy describes one in-process dispatch to the ARM handler: which resource types and operations the page may
// reach. Everything else answers 400, so the proxy never forwards arbitrary ARM calls.
type armProxy struct {
	allowed   func(method string, p []string) bool
	name      string
	prefix    string
	queryKeys []string
	methods   []string
}

func (h *DashboardHandler) registerARMProxy(px armProxy) {
	var inner *echo.Echo

	if h.config.AzureARMOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.AzureARMOps.Handler())
	}

	serve := func(c *echo.Context) error {
		if inner == nil {
			return azureACIFail(c, http.StatusServiceUnavailable, errServiceUnavailable, px.name+" is not enabled")
		}

		r := c.Request()

		armPath, ok := armProxyPath(r.Method, strings.TrimPrefix(r.URL.EscapedPath(), px.prefix), px.allowed)
		if !ok {
			return azureACIFail(c, http.StatusBadRequest, "InvalidRequestUri", "unsupported "+px.name+" proxy request")
		}

		q := url.Values{}

		for _, k := range px.queryKeys {
			if v := r.URL.Query().Get(k); v != "" {
				q.Set(k, v)
			}
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(armPath), armPath
		fwd.URL.RawQuery = q.Encode()
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, armPxMaxBody)

		rec := httptest.NewRecorder()
		inner.ServeHTTP(rec, fwd)

		return armProxyRelay(c, rec)
	}

	for _, m := range px.methods {
		h.SubRouter.Add(m, px.prefix+"/*", serve)
	}
}

// armProxyPath validates the proxied ARM path segments and the allowlist.
func armProxyPath(method, raw string, allowed func(string, []string) bool) (string, bool) {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.ContainsAny(dec, "/:") {
			return "", false
		}
	}

	if !allowed(method, strings.Split(strings.ToLower(strings.TrimPrefix(raw, "/")), "/")) {
		return "", false
	}

	return "/" + strings.Join(parts, "/"), true
}

// armProxyRelay copies the ARM response, removing secret-bearing properties before they reach the browser.
func armProxyRelay(c *echo.Context, rec *httptest.ResponseRecorder) error {
	body := rec.Body.Bytes()

	if rec.Code < http.StatusMultipleChoices && len(body) > 0 {
		var doc any
		if err := json.Unmarshal(body, &doc); err == nil {
			armScrub(doc)

			if out, merr := json.Marshal(doc); merr == nil {
				body = out
			}
		}
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(rec.Code, body)
}

func armScrub(v any) {
	switch t := v.(type) {
	case []any:
		for _, x := range t {
			armScrub(x)
		}
	case map[string]any:
		for k, x := range t {
			switch {
			case slices.Contains(armPxSecretKeys, strings.ToLower(k)):
				delete(t, k)
			case k == armPxSecretsKy:
				armDropValues(x)
			default:
				armScrub(x)
			}
		}

	}
}

// armDropValues removes the value of every entry of a secrets array.
func armDropValues(v any) {
	list, ok := v.([]any)
	if !ok {
		return
	}

	for _, e := range list {
		if m, isMap := e.(map[string]any); isMap {
			delete(m, "value")
		}
	}
}

// armScope splits a lower-cased proxied path below /subscriptions/{s} into the resource type and the segments after
// the resource name. A subscription-level list (/providers/{ns}/{type}) has no rest and name "".
func armScope(p []string, ns string) (typ, name string, rest []string, ok bool) {
	if len(p) < armPxSubList || p[0] != armPxSubsSeg {
		return "", "", nil, false
	}

	if len(p) == armPxSubList {
		return p[armPxTypeSub], "", nil, p[2] == armPxProvSeg && p[armPxNSSub] == ns
	}

	if len(p) < armPxItem || p[2] != armPxRGsSeg || p[4] != armPxProvSeg || p[armPxNSRG] != ns {
		return "", "", nil, false
	}

	return p[armPxTypeRG], p[armPxItem-1], p[armPxItem:], true
}
