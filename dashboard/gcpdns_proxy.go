package dashboard

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	gcpDNSProxyPrefix  = "/dashboard/api/v1/gcp/dns"
	gcpDNSMaxBodyBytes = 1 << 20
	gcpDNSAPIPrefix    = "/dns"
)

// registerGCPDNSRoutes exposes the Cloud DNS managed zones, record sets and changes to the SPA by dispatching
// in-process to the emulator's handler, so every response keeps the real REST shape.
func (h *DashboardHandler) registerGCPDNSRoutes() {
	var inner *echo.Echo

	if h.config.GCPDNSOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.GCPDNSOps.Handler())
	}

	serve := func(c *echo.Context) error {
		if inner == nil {
			return cloudUnavailable(c, "Cloud DNS")
		}

		r := c.Request()

		apiPath, ok := gcpDNSProxyPath(r.Method, strings.TrimPrefix(r.URL.EscapedPath(), gcpDNSProxyPrefix))
		if !ok {
			return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, "unsupported Cloud DNS proxy request")
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(apiPath), apiPath
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, gcpDNSMaxBodyBytes)
		inner.ServeHTTP(c.Response(), fwd)

		return nil
	}

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		h.SubRouter.Add(method, gcpDNSProxyPrefix+"/*", serve)
	}
}

// gcpDNSProxyPath maps /v1/projects/<p>/managedZones[/...] to the emulator's /dns/v1 path. Only managed zones,
// their record sets and changes are reachable, and writes are limited to record sets and changes.
func gcpDNSProxyPath(method, raw string) (string, bool) {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")
	if len(parts) < 4 || parts[0] != "v1" || parts[1] != "projects" || parts[3] != "managedZones" {
		return "", false
	}

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.Contains(dec, "/") {
			return "", false
		}
	}

	if method != http.MethodGet {
		const writeKindIdx = 5
		// zone-level deletes and creates are allowed (zones with record sets and changes), not policies etc.
		if len(parts) > writeKindIdx && parts[5] != "rrsets" && parts[5] != "changes" {
			return "", false
		}
	}

	return gcpDNSAPIPrefix + "/" + strings.Join(parts, "/"), true
}
