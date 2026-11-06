package dashboard

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/lambdaweb"
)

const (
	lambdaWebPrefix    = "/dashboard/api/v1/lambdaweb"
	lambdaWebAPI       = "/2025-03-07/web-functions"
	lambdaWebMaxBody   = 1 << 17
	lambdaWebMaxInvoke = 2 * lambdaWebMaxBody
	lambdaWebMaxReply  = 256 << 10
	lambdaWebMaxPath   = 2048
	lambdaWebMaxHeader = 24
	lambdaWebMaxValue  = 4096
	lambdaWebPageSize  = 50
	lambdaWebHidden    = "<hidden>"
)

var (
	lambdaWebNameRe   = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,140}$`)
	lambdaWebRegionRe = regexp.MustCompile(`^[a-z0-9-]{1,32}$`)
	lambdaWebHeaderRe = regexp.MustCompile("^[A-Za-z0-9!#$%&'*+.^_`|~-]{1,64}$")
	lambdaWebTokenRe  = regexp.MustCompile(`^[A-Za-z0-9+/=_.:-]{0,1000}$`)
)

// lambdaWebBlocked are request headers the invoke panel never forwards.
//
//nolint:gochecknoglobals // immutable deny-list
var lambdaWebBlocked = []string{
	"host", "connection", "content-length", "transfer-encoding", "upgrade", "expect", "te", "trailer",
	"keep-alive", "proxy-authorization", "proxy-connection", "x-real-ip", "x-forwarded-for", "x-forwarded-host",
}

// registerLambdaWebRoutes exposes Lambda Web Functions by dispatching in-process to its handler. Reads are fixed
// control plane calls; the invoke panel calls an endpoint domain through the same handler, so no network is touched.
func (h *DashboardHandler) registerLambdaWebRoutes() {
	var inner *echo.Echo

	if h.config.LambdaWebOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.LambdaWebOps.Handler())
	}

	lw := lambdaWebAccess{inner: inner}
	guard := func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			if inner == nil {
				return azureACIFail(c, http.StatusServiceUnavailable, errServiceUnavailable,
					"Lambda Web Functions is not enabled")
			}

			return next(c)
		}
	}

	h.SubRouter.Add(http.MethodGet, lambdaWebPrefix+"/functions", guard(lw.list("", "")))
	h.SubRouter.Add(http.MethodGet, lambdaWebPrefix+"/functions/:fn", guard(lw.get("")))
	h.SubRouter.Add(
		http.MethodGet,
		lambdaWebPrefix+"/functions/:fn/revisions",
		guard(lw.list("revisions", "list-revisions")),
	)
	h.SubRouter.Add(http.MethodGet, lambdaWebPrefix+"/functions/:fn/revisions/:child", guard(lw.get("revisions")))
	h.SubRouter.Add(
		http.MethodGet,
		lambdaWebPrefix+"/functions/:fn/endpoints",
		guard(lw.list("endpoints", "list-endpoints")),
	)
	h.SubRouter.Add(http.MethodGet, lambdaWebPrefix+"/functions/:fn/endpoints/:child", guard(lw.get("endpoints")))
	h.SubRouter.Add(http.MethodPost, lambdaWebPrefix+"/invoke", guard(lw.invoke))
}

type lambdaWebAccess struct{ inner *echo.Echo }

func lambdaWebBad(c *echo.Context) error {
	return azureACIFail(c, http.StatusBadRequest, "InvalidRequestUri", "invalid Lambda Web Functions request")
}

func (lw lambdaWebAccess) do(c *echo.Context, region, method, path string, body []byte) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(c.Request().Context(), method, path, bytes.NewReader(body))
	if region != "" {
		req.Header.Set("X-Amz-Region", region)
	}

	if len(body) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	lw.inner.ServeHTTP(rec, req)

	return rec
}

func lambdaWebRegion(c *echo.Context) (string, bool) {
	region := c.QueryParam("region")

	return region, region == "" || lambdaWebRegionRe.MatchString(region)
}

// lambdaWebView hides environment variable values, which commonly hold secrets.
func lambdaWebView(v any) {
	switch t := v.(type) {
	case []any:
		for _, x := range t {
			lambdaWebView(x)
		}
	case map[string]any:
		for k, x := range t {
			if m, ok := x.(map[string]any); ok && k == "environmentVariables" {
				for name := range m {
					m[name] = lambdaWebHidden
				}

				continue
			}

			lambdaWebView(x)
		}
	}
}

func lambdaWebRelay(c *echo.Context, rec *httptest.ResponseRecorder) error {
	body := rec.Body.Bytes()
	if len(body) == 0 {
		return c.NoContent(rec.Code)
	}

	if rec.Code < http.StatusMultipleChoices {
		body = armScrubBody(body, lambdaWebView)
	} else if t := rec.Header().Get("X-Amzn-Errortype"); t != "" {
		c.Response().Header().Set("X-Amzn-Errortype", t)
	}

	return c.JSONBlob(rec.Code, body)
}

// list serves a function list (child "") or a list of a function's revisions or endpoints.
func (lw lambdaWebAccess) list(child, op string) echo.HandlerFunc {
	return func(c *echo.Context) error {
		region, ok := lambdaWebRegion(c)
		token := c.QueryParam("nextToken")

		if !ok || !lambdaWebTokenRe.MatchString(token) {
			return lambdaWebBad(c)
		}

		path := lambdaWebAPI

		if child != "" {
			fn := c.Param("fn")
			if !lambdaWebNameRe.MatchString(fn) {
				return lambdaWebBad(c)
			}

			path += "/" + url.PathEscape(fn) + "/" + op
		}

		in := map[string]any{"maxResults": lambdaWebPageSize}
		if token != "" {
			in["nextToken"] = token
		}

		body, err := json.Marshal(in)
		if err != nil {
			return lambdaWebBad(c)
		}

		return lambdaWebRelay(c, lw.do(c, region, http.MethodPost, path, body))
	}
}

// get serves one function (child "") or one revision or endpoint of it.
func (lw lambdaWebAccess) get(child string) echo.HandlerFunc {
	return func(c *echo.Context) error {
		region, ok := lambdaWebRegion(c)
		fn := c.Param("fn")

		if !ok || !lambdaWebNameRe.MatchString(fn) {
			return lambdaWebBad(c)
		}

		path := lambdaWebAPI + "/" + url.PathEscape(fn)

		if child != "" {
			name := c.Param("child")
			if !lambdaWebNameRe.MatchString(name) {
				return lambdaWebBad(c)
			}

			path += "/" + child + "/" + url.PathEscape(name)
		}

		return lambdaWebRelay(c, lw.do(c, region, http.MethodGet, path, nil))
	}
}

type lambdaWebHeaderIn struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type lambdaWebInvokeIn struct {
	Region   string              `json:"region"`
	Function string              `json:"function"`
	Endpoint string              `json:"endpoint"`
	Method   string              `json:"method"`
	Path     string              `json:"path"`
	Body     string              `json:"body"`
	Headers  []lambdaWebHeaderIn `json:"headers"`
}

func (in lambdaWebInvokeIn) valid() bool {
	if !lambdaWebNameRe.MatchString(in.Function) || !lambdaWebNameRe.MatchString(in.Endpoint) ||
		in.Region != "" && !lambdaWebRegionRe.MatchString(in.Region) {
		return false
	}

	methods := []string{
		http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
		http.MethodOptions,
	}

	if !slices.Contains(methods, in.Method) || len(in.Body) > lambdaWebMaxBody {
		return false
	}

	if !strings.HasPrefix(in.Path, "/") || strings.HasPrefix(in.Path, "//") || len(in.Path) > lambdaWebMaxPath ||
		strings.ContainsFunc(in.Path, func(r rune) bool { return r <= ' ' || r == 0x7f }) {
		return false
	}

	return in.headersValid()
}

func (in lambdaWebInvokeIn) headersValid() bool {
	if len(in.Headers) > lambdaWebMaxHeader {
		return false
	}

	for _, h := range in.Headers {
		if !lambdaWebHeaderRe.MatchString(h.Name) || slices.Contains(lambdaWebBlocked, strings.ToLower(h.Name)) ||
			len(h.Value) > lambdaWebMaxValue || strings.ContainsAny(h.Value, "\r\n\x00") {
			return false
		}
	}

	return true
}

type lambdaWebReply struct {
	Headers    map[string][]string `json:"headers"`
	Body       string              `json:"body"`
	Encoding   string              `json:"encoding"`
	Domain     string              `json:"domain"`
	DurationMs int64               `json:"durationMs"`
	Status     int                 `json:"status"`
	Truncated  bool                `json:"truncated"`
}

// invoke sends one request to the domain of an endpoint and returns what came back.
func (lw lambdaWebAccess) invoke(c *echo.Context) error {
	var in lambdaWebInvokeIn

	dec := json.NewDecoder(http.MaxBytesReader(c.Response(), c.Request().Body, lambdaWebMaxInvoke))
	dec.DisallowUnknownFields()

	if dec.Decode(&in) != nil || !in.valid() {
		return azureACIFail(c, http.StatusBadRequest, "InvalidRequestContent", "the invoke request is invalid")
	}

	epPath := lambdaWebAPI + "/" + url.PathEscape(in.Function) + "/endpoints/" + url.PathEscape(in.Endpoint)

	ep := lw.do(c, in.Region, http.MethodGet, epPath, nil)
	if ep.Code != http.StatusOK {
		return azureACIFail(c, ep.Code, "ResourceNotFound", "the endpoint was not found")
	}

	var info struct {
		Domain string `json:"domainName"`
	}
	if json.Unmarshal(ep.Body.Bytes(), &info) != nil || !lambdaweb.IsEndpointHost(info.Domain) {
		return azureACIFail(c, http.StatusBadGateway, "EndpointUnavailable", "the endpoint has no domain")
	}

	req, err := http.NewRequestWithContext(c.Request().Context(), in.Method, "http://"+info.Domain+in.Path,
		strings.NewReader(in.Body))
	if err != nil {
		return azureACIFail(c, http.StatusBadRequest, "InvalidRequestContent", "the request path is invalid")
	}

	for _, h := range in.Headers {
		req.Header.Add(h.Name, h.Value)
	}

	start := time.Now()
	rec := httptest.NewRecorder()
	lw.inner.ServeHTTP(rec, req)

	out := lambdaWebReply{
		Status: rec.Code, Headers: rec.Header().Clone(), DurationMs: time.Since(start).Milliseconds(),
		Domain: info.Domain,
	}

	raw, _ := io.ReadAll(io.LimitReader(rec.Body, lambdaWebMaxReply+1))
	if len(raw) > lambdaWebMaxReply {
		raw, out.Truncated = raw[:lambdaWebMaxReply], true
	}

	if utf8.Valid(raw) {
		out.Body, out.Encoding = string(raw), "text"
	} else {
		out.Body, out.Encoding = base64.StdEncoding.EncodeToString(raw), "base64"
	}

	return c.JSON(http.StatusOK, out)
}
