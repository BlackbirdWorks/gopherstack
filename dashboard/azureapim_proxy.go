package dashboard

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	azureAPIMPrefix  = "/dashboard/api/v1/azure/apim"
	apimPxNamespace  = "microsoft.apimanagement"
	apimPxService    = "service"
	apimPxVersion    = "2024-05-01"
	apimPxGateway    = "/_apim/"
	apimPxInvoke     = "invoke"
	apimPxInvokeSegs = 9
	apimPxNameIdx    = 7
	apimPxVerbIdx    = 8
	apimPxPolicy     = "policy"
	apimPxPolicies   = "policies"
	apimPxAPIs       = "apis"
	apimPxActive     = "active"
	apimPxKeyHeader  = "Ocp-Apim-Subscription-Key"
	apimPxKeyQuery   = "subscription-key"
	apimPxPrimaryKey = "primaryKey"
	apimPxSecret     = "secret"
	apimPxMaxBody    = ociAPIGWMaxTryJSON + 1<<12
)

//nolint:gochecknoglobals // immutable allow-lists
var (
	apimPxShapes = []string{
		"apis", "apis/*", "apis/*/operations", "apis/*/operations/*", "apis/*/policies", "apis/*/policies/policy",
		"apis/*/operations/*/policies/policy", "products", "products/*", "products/*/apis", armPxSubsSeg,
		armPxSubsSeg + "/*", "namedvalues", "namedvalues/*", "policies/policy",
	}
	apimPxNameRE  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,89}$`)
	apimPxQueries = []string{armQueryVersion, "format", "$top", "$skip"}
)

// registerAzureAPIMRoutes serves the API Management page by dispatching in-process to the ARM handler. Only reads of
// services, APIs, operations, products, subscriptions, named values and policies are reachable (never listSecrets),
// and a "try it" call replays one request through the gateway of a selected service with the chosen subscription's
// key injected server-side; keys and secret named-value values never reach the browser.
func (h *DashboardHandler) registerAzureAPIMRoutes() {
	h.registerARMProxy(armProxy{
		name: "API Management", prefix: azureAPIMPrefix,
		queryKeys: apimPxQueries,
		methods:   []string{http.MethodGet},
		allowed:   apimProxyAllowed,
		view:      apimProxyView,
	})

	var inner *echo.Echo

	if h.config.AzureARMOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.AzureARMOps.Handler())
	}

	h.SubRouter.Add(http.MethodPost, azureAPIMPrefix+"/*", func(c *echo.Context) error {
		if inner == nil {
			return azureACIFail(
				c, http.StatusServiceUnavailable, errServiceUnavailable, "API Management is not enabled",
			)
		}

		return (&apimTry{c: c, inner: inner}).serve()
	})
}

func apimProxyAllowed(method string, p []string) bool {
	if method != http.MethodGet {
		return false
	}

	typ, name, rest, ok := armScope(p, apimPxNamespace)
	if !ok || typ != apimPxService {
		return false
	}

	if name == "" || len(rest) == 0 {
		return true
	}

	return apimProxyChild(rest)
}

// apimProxyChild reports whether the segments below a service form one of the readable shapes; resource ids are
// wildcards, except the fixed policy id.
func apimProxyChild(rest []string) bool {
	shape := make([]string, len(rest))

	for i, seg := range rest {
		shape[i] = seg
		if i%2 == 1 && rest[i-1] != apimPxPolicies {
			shape[i] = "*"
		}
	}

	return slices.Contains(apimPxShapes, strings.Join(shape, "/"))
}

// apimProxyView masks named-value secrets before the generic scrub drops the secret flag.
func apimProxyView(doc any) {
	switch t := doc.(type) {
	case []any:
		for _, x := range t {
			apimProxyView(x)
		}
	case map[string]any:
		if props, ok := t["properties"].(map[string]any); ok {
			if sec, _ := props[apimPxSecret].(bool); sec {
				props["isSecret"] = true

				delete(props, "value")
			}
		}

		for _, x := range t {
			if _, isMap := x.(map[string]any); isMap {
				apimProxyView(x)
			} else if _, isList := x.([]any); isList {
				apimProxyView(x)
			}
		}
	}
}

type apimTryIn struct {
	Subscription string `json:"subscription"`
	API          string `json:"api"`
	ociAPIGWTryIn
}

type apimTry struct {
	c     *echo.Context
	inner *echo.Echo
}

func (t *apimTry) fail(status int, code, msg string) error {
	return azureACIFail(t.c, status, code, msg)
}

func (t *apimTry) serve() error {
	parts, ok := apimTryParts(t.c.Request().URL.EscapedPath())
	if !ok {
		return t.fail(http.StatusBadRequest, "InvalidRequestUri", "unsupported API Management proxy request")
	}

	in, rel, ok := t.decode()
	if !ok {
		return t.fail(http.StatusBadRequest, "InvalidRequestContent", "invalid API Management try-it request")
	}

	svc := "/" + strings.Join(parts[:apimPxVerbIdx], "/")
	ctx := t.c.Request().Context()

	if rec := t.arm(ctx, http.MethodGet, svc); rec.Code != http.StatusOK {
		return t.relayError(rec)
	}

	key, status, msg := t.subscriptionKey(ctx, svc, in.Subscription)
	if status != 0 {
		return t.fail(status, "SubscriptionUnusable", msg)
	}

	return t.replay(ctx, replayArgs{
		name: parts[apimPxNameIdx], rel: rel, in: in, key: key, api: t.apiInfo(ctx, svc, in.API),
	})
}

// apimTryParts validates the escaped path below the proxy prefix: .../service/{name}/invoke.
func apimTryParts(escaped string) ([]string, bool) {
	parts := strings.Split(strings.TrimPrefix(escaped, azureAPIMPrefix+"/"), "/")
	if len(parts) != apimPxInvokeSegs {
		return nil, false
	}

	for i, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || (i%2 == 1 && !apimPxNameRE.MatchString(dec)) {
			return nil, false
		}
	}

	return parts, apimTryShape(strings.Split(strings.ToLower(strings.Join(parts, "/")), "/"))
}

// decode reads the try-it body and returns it with the validated "/path?query" relative to the gateway.
func (t *apimTry) decode() (apimTryIn, string, bool) {
	var in apimTryIn
	if !ociDecodeBodyN(t.c, &in, apimPxMaxBody) {
		return in, "", false
	}

	rel, ok := ociAPIGWTryPath(in.ociAPIGWTryIn)
	ok = ok && ociAPIGWMethodOK(in.Method) && len(in.Headers) <= ociAPIGWMaxHeaders &&
		len(in.Body) <= ociAPIGWMaxReqBody &&
		(in.Subscription == "" || apimPxNameRE.MatchString(in.Subscription)) &&
		(in.API == "" || apimPxNameRE.MatchString(in.API))

	return in, rel, ok
}

func apimTryShape(low []string) bool {
	return low[0] == armPxSubsSeg && low[2] == armPxRGsSeg && low[4] == armPxProvSeg && low[5] == apimPxNamespace &&
		low[6] == apimPxService && low[apimPxVerbIdx] == apimPxInvoke
}

func (t *apimTry) relayError(rec *httptest.ResponseRecorder) error {
	t.c.Response().Header().Set("Content-Type", "application/json")

	return t.c.JSONBlob(rec.Code, rec.Body.Bytes())
}

// arm performs one ARM call against the in-process handler with no caller-supplied bytes.
func (t *apimTry) arm(ctx context.Context, method, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(ctx, method, path+"?"+armQueryVersion+"="+apimPxVersion,
		bytes.NewReader([]byte("{}")))
	req.Header.Set("Content-Type", echo.MIMEApplicationJSON)

	rec := httptest.NewRecorder()
	t.inner.ServeHTTP(rec, req)

	return rec
}

// subscriptionKey resolves the primary key of an active subscription; empty with status 0 when none was selected.
func (t *apimTry) subscriptionKey(ctx context.Context, svc, sub string) (string, int, string) {
	if sub == "" {
		return "", 0, ""
	}

	base := svc + "/subscriptions/" + url.PathEscape(sub)

	rec := t.arm(ctx, http.MethodGet, base)
	if rec.Code != http.StatusOK {
		return "", http.StatusNotFound, "Subscription not found"
	}

	var doc struct {
		Properties map[string]any `json:"properties"`
	}

	if json.Unmarshal(rec.Body.Bytes(), &doc) != nil || doc.Properties["state"] != apimPxActive {
		return "", http.StatusConflict, "Subscription is not active"
	}

	rec = t.arm(ctx, http.MethodPost, base+"/listSecrets")

	var keys map[string]any
	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &keys) != nil {
		return "", http.StatusConflict, "Subscription key is unavailable"
	}

	key, _ := keys[apimPxPrimaryKey].(string)
	if key == "" {
		return "", http.StatusConflict, "Subscription key is unavailable"
	}

	return key, 0, ""
}

// apimAPIInfo is what the replay needs from the selected API: its key parameter names and whether it is served over
// TLS (an API limited to http is replayed without TLS, as the gateway matches the protocol).
type apimAPIInfo struct {
	header string
	query  string
	tls    bool
}

func (t *apimTry) apiInfo(ctx context.Context, svc, api string) apimAPIInfo {
	def := apimAPIInfo{header: apimPxKeyHeader, query: apimPxKeyQuery, tls: true}
	if api == "" {
		return def
	}

	rec := t.arm(ctx, http.MethodGet, svc+"/"+apimPxAPIs+"/"+url.PathEscape(api))

	var doc struct {
		Properties struct {
			Names struct {
				Header string `json:"header"`
				Query  string `json:"query"`
			} `json:"subscriptionKeyParameterNames"`
			Protocols []string `json:"protocols"`
		} `json:"properties"`
	}

	if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &doc) != nil {
		return def
	}

	n := doc.Properties.Names
	def.header, def.query = orDefault(headerToken(n.Header), apimPxKeyHeader), orDefault(n.Query, apimPxKeyQuery)

	if p := doc.Properties.Protocols; len(p) > 0 {
		def.tls = slices.ContainsFunc(p, func(s string) bool { return strings.EqualFold(s, "https") })
	}

	return def
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}

	return v
}

func headerToken(name string) string {
	if !ociAPIGWHeaderOK(name) {
		return ""
	}

	return name
}

type replayArgs struct {
	name string
	rel  string
	key  string
	in   apimTryIn
	api  apimAPIInfo
}

func (t *apimTry) replay(ctx context.Context, a replayArgs) error {
	name, rel, in, key := a.name, a.rel, a.in, a.key
	hdrName, queryName := a.api.header, a.api.query
	path, rawQuery, _ := strings.Cut(rel, "?")
	q, _ := url.ParseQuery(rawQuery)

	if key != "" {
		q.Del(queryName)
	}

	target := apimPxGateway + url.PathEscape(name) + path
	if len(q) > 0 {
		target += "?" + q.Encode()
	}

	req := httptest.NewRequestWithContext(ctx, in.Method, target, io.NopCloser(strings.NewReader(in.Body)))
	if a.api.tls {
		req.TLS = &tls.ConnectionState{}
	}

	for _, hd := range in.Headers {
		if ociAPIGWHeaderOK(hd.Name) && (key == "" || !strings.EqualFold(hd.Name, hdrName)) {
			req.Header.Add(hd.Name, hd.Value)
		}
	}

	if key != "" {
		req.Header.Set(hdrName, key)
	}

	rec := httptest.NewRecorder()
	t.inner.ServeHTTP(rec, req)

	return t.c.JSON(http.StatusOK, ociAPIGWTryOut(rec, key))
}
