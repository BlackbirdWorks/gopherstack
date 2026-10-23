package dashboard

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	gcpBigQueryProxyPrefix  = "/dashboard/api/v1/gcp/bigquery"
	gcpBigQueryMaxBodyBytes = 4 << 20
	gcpBigQueryAPIPrefix    = "bigquery/v2"
)

// registerGCPBigQueryRoutes exposes the BigQuery emulator to the SPA by dispatching
// in-process to its handler, avoiding its separate listener and CORS.
func (h *DashboardHandler) registerGCPBigQueryRoutes() {
	var router *echo.Echo

	if h.config.GCPBigQueryOps != nil {
		router = echo.New()
		router.Any("/*", h.config.GCPBigQueryOps.Handler())
	}

	serve := func(c *echo.Context) error {
		if router == nil {
			return c.JSON(http.StatusServiceUnavailable, map[string]string{keyError: errServiceUnavailable})
		}

		r := c.Request()
		escaped := strings.TrimPrefix(r.URL.EscapedPath(), gcpBigQueryProxyPrefix)

		p, ok := gcpBigQueryProxyPath(r.Method, escaped)
		if !ok {
			return c.String(http.StatusBadRequest, "unsupported BigQuery proxy request")
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path = p
		fwd.URL.RawPath = escaped
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, gcpBigQueryMaxBodyBytes)
		router.ServeHTTP(c.Response(), fwd)

		return nil
	}

	for _, method := range []string{
		http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodDelete,
	} {
		h.SubRouter.Add(method, gcpBigQueryProxyPrefix+"/*", serve)
	}
}

// gcpBigQueryProxyPath admits only the allowlisted REST shapes under /bigquery/v2
// for the given method and returns the decoded path.
func gcpBigQueryProxyPath(method, escaped string) (string, bool) {
	rest, ok := strings.CutPrefix(escaped, "/"+gcpBigQueryAPIPrefix+"/")
	if !ok {
		return "", false
	}

	segs, ok := gcpBigQuerySegments(rest)
	if !ok || !gcpBigQueryMethodAllowed(method, gcpBigQueryPattern(segs)) {
		return "", false
	}

	return "/" + gcpBigQueryAPIPrefix + "/" + strings.Join(segs, "/"), true
}

func gcpBigQuerySegments(rest string) ([]string, bool) {
	parts := strings.Split(rest, "/")
	segs := make([]string, len(parts))

	for i, part := range parts {
		s, err := url.PathUnescape(part)
		if err != nil || s == "" || s == "." || s == ".." || strings.Contains(s, "/") {
			return nil, false
		}

		segs[i] = s
	}

	return segs, true
}

// gcpBigQueryPattern replaces resource ids (odd positions) with "*".
func gcpBigQueryPattern(segs []string) string {
	out := make([]string, len(segs))

	for i, s := range segs {
		if i%2 == 1 {
			s = "*"
		}

		out[i] = s
	}

	return strings.Join(out, "/")
}

func gcpBigQueryMethodAllowed(method, pattern string) bool {
	switch pattern {
	case "projects", "projects/*/datasets/*/tables/*/data", "projects/*/jobs/*", "projects/*/queries/*":
		return method == http.MethodGet
	case "projects/*/datasets", "projects/*/datasets/*/tables", "projects/*/jobs":
		return method == http.MethodGet || method == http.MethodPost
	case "projects/*/datasets/*", "projects/*/datasets/*/tables/*":
		return method == http.MethodGet || method == http.MethodPatch || method == http.MethodDelete
	case "projects/*/queries":
		return method == http.MethodPost
	default:
		return false
	}
}
