package dashboard

import (
	"bytes"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strings"
	"unicode/utf8"

	"github.com/labstack/echo/v5"
)

const (
	ociAPIGWPrefix      = ociProxyPrefix + "/apigateway"
	ociAPIGWAPIPrefix   = "/20190501"
	ociAPIGWDataPrefix  = "/_apigw/"
	ociAPIGWItemSegs    = 2
	ociAPIGWInvokeSegs  = 3
	ociAPIGWMaxReqBody  = 1 << 16
	ociAPIGWMaxRespBody = 1 << 18
	ociAPIGWMaxHeaders  = 32
	ociAPIGWMaxPath     = 2048
	ociAPIGWActiveState = "ACTIVE"
	ociAPIGWMaxTryJSON  = 2 * ociAPIGWMaxReqBody
	ociAPIGWGateways    = "gateways"
	ociAPIGWDeployments = "deployments"
	ociAPIGWCerts       = "certificates"
)

// registerOCIAPIGatewayRoutes serves the API Gateway page by dispatching in-process to the emulator's handler without
// request signing. Only gateway, deployment and certificate reads are reachable, plus a "try it" call that is
// replayed against the in-process data plane of one existing gateway selected by id; the destination is never taken
// from the request. Certificate private keys are discarded by the emulator.
func (h *DashboardHandler) registerOCIAPIGatewayRoutes() {
	var (
		inner *echo.Echo
		comps func() []string
	)

	ops := h.config.OCIAPIGatewayOps
	if ops != nil {
		inner = echo.New()
		inner.Any("/*", ops.Trusted())
		comps = ops.Backend.Compartments
	}

	guard := func(next echo.HandlerFunc) echo.HandlerFunc { return ociGuard(inner != nil, "OCI API Gateway", next) }

	h.SubRouter.Add(http.MethodGet, ociAPIGWPrefix+"/compartments", guard(func(c *echo.Context) error {
		return c.JSON(http.StatusOK, map[string]any{ociCompartmentsKey: comps()})
	}))

	serve := guard(func(c *echo.Context) error {
		r := c.Request()
		raw := strings.TrimPrefix(r.URL.EscapedPath(), ociAPIGWPrefix+"/api")

		parts, ok := ociSplitPath(raw)
		if !ok {
			return ociInvalid(c, "unsupported API Gateway proxy request")
		}

		if r.Method == http.MethodPost && len(parts) == ociAPIGWInvokeSegs && parts[0] == ociAPIGWGateways &&
			parts[2] == "invoke" {
			return ociAPIGWTryIt(c, inner, ops.Backend.GatewayState, parts[1])
		}

		if r.Method != http.MethodGet || !ociAPIGWReadable(parts) {
			return ociInvalid(c, "unsupported API Gateway proxy request")
		}

		return ociDispatch(c, inner, ociAPIGWAPIPrefix+raw, nil)
	})

	h.SubRouter.Add(http.MethodGet, ociAPIGWPrefix+"/api/*", serve)
	h.SubRouter.Add(http.MethodPost, ociAPIGWPrefix+"/api/*", serve)
}

func ociAPIGWReadable(parts []string) bool {
	switch len(parts) {
	case 1:
		return slices.Contains([]string{ociAPIGWGateways, ociAPIGWDeployments, ociAPIGWCerts}, parts[0])
	case ociAPIGWItemSegs:
		return slices.Contains([]string{ociAPIGWGateways, ociAPIGWDeployments}, parts[0])
	}

	return false
}

type ociAPIGWTryHeader struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type ociAPIGWTryIn struct {
	Method  string              `json:"method"`
	Path    string              `json:"path"`
	Query   string              `json:"query"`
	Body    string              `json:"body"`
	Headers []ociAPIGWTryHeader `json:"headers"`
}

// ociAPIGWHopByHop are never forwarded in either direction (RFC 9110 section 7.6.1, plus Host and Content-Length,
// which the proxy sets itself).
var ociAPIGWHopByHop = []string{ //nolint:gochecknoglobals // immutable header deny-list
	"Connection", "Keep-Alive", "Proxy-Authenticate", "Proxy-Authorization", "Proxy-Connection", "Te", "Trailer",
	"Transfer-Encoding", "Upgrade", "Host", "Content-Length",
}

func ociAPIGWTryIt(c *echo.Context, inner *echo.Echo, state func(string) (string, bool), gatewayID string) error {
	var in ociAPIGWTryIn
	if !ociDecodeBodyN(c, &in, ociAPIGWMaxTryJSON) {
		return ociInvalid(c, "invalid API Gateway try-it request")
	}

	rel, ok := ociAPIGWTryPath(in)
	if !ok || !ociAPIGWMethodOK(in.Method) || len(in.Headers) > ociAPIGWMaxHeaders ||
		len(in.Body) > ociAPIGWMaxReqBody {
		return ociInvalid(c, "invalid API Gateway try-it request")
	}

	st, found := state(gatewayID)
	if !found {
		return cloudJSONFail(c, http.StatusNotFound, "NotAuthorizedOrNotFound", "Gateway not found")
	}

	if st != ociAPIGWActiveState {
		return cloudJSONFail(c, http.StatusConflict, "IncorrectState", "Gateway is in state "+st)
	}

	req := httptest.NewRequestWithContext(c.Request().Context(), in.Method,
		ociAPIGWDataPrefix+url.PathEscape(gatewayID)+rel, strings.NewReader(in.Body))
	req.Host = "dashboard.invalid"

	for _, hd := range in.Headers {
		if ociAPIGWHeaderOK(hd.Name) {
			req.Header.Add(hd.Name, hd.Value)
		}
	}

	rec := httptest.NewRecorder()
	inner.ServeHTTP(rec, req)

	return c.JSON(http.StatusOK, ociAPIGWTryOut(rec))
}

func ociAPIGWMethodOK(m string) bool {
	return slices.Contains([]string{
		http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
		http.MethodHead, http.MethodOptions,
	}, m)
}

// ociAPIGWTryPath validates the path and query and returns "/path?query" relative to the gateway.
func ociAPIGWTryPath(in ociAPIGWTryIn) (string, bool) {
	if !strings.HasPrefix(in.Path, "/") || len(in.Path)+len(in.Query) > ociAPIGWMaxPath {
		return "", false
	}

	if in.Path != "/" {
		if _, ok := ociSplitPath(in.Path); !ok {
			return "", false
		}
	}

	if strings.ContainsAny(in.Path, "?#\\") || strings.ContainsAny(in.Query, "#\r\n") {
		return "", false
	}

	if _, err := url.ParseQuery(in.Query); err != nil {
		return "", false
	}

	if in.Query == "" {
		return in.Path, true
	}

	return in.Path + "?" + in.Query, true
}

func ociAPIGWHeaderOK(name string) bool {
	if name == "" || strings.ContainsAny(name, " :\r\n\t") {
		return false
	}

	return !slices.ContainsFunc(ociAPIGWHopByHop, func(h string) bool { return strings.EqualFold(h, name) })
}

// ociAPIGWTryOut renders a data-plane response for the browser; every occurrence of a secret is masked.
func ociAPIGWTryOut(rec *httptest.ResponseRecorder, secrets ...string) map[string]any {
	headers := map[string]string{}

	for k, v := range rec.Header() {
		if ociAPIGWHeaderOK(k) {
			headers[k] = ociAPIGWMask(strings.Join(v, ", "), secrets)
		}
	}

	raw := ociAPIGWMaskBytes(rec.Body.Bytes()[:min(rec.Body.Len(), ociAPIGWMaxRespBody)], secrets)
	out := map[string]any{
		"status": rec.Code, "headers": headers, "truncated": rec.Body.Len() > ociAPIGWMaxRespBody,
	}

	if utf8.Valid(raw) {
		out["body"], out["encoding"] = string(raw), "utf8"
	} else {
		out["body"], out["encoding"] = base64.StdEncoding.EncodeToString(raw), "base64"
	}

	return out
}

func ociAPIGWMask(s string, secrets []string) string {
	for _, sec := range secrets {
		if sec != "" {
			s = strings.ReplaceAll(s, sec, "[redacted]")
		}
	}

	return s
}

func ociAPIGWMaskBytes(b []byte, secrets []string) []byte {
	for _, sec := range secrets {
		if sec != "" {
			b = bytes.ReplaceAll(b, []byte(sec), []byte("[redacted]"))
		}
	}

	return b
}
