package dashboard

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	gcpSQLProxyPrefix  = "/dashboard/api/v1/gcp/sql"
	gcpSQLAPIPrefix    = "/sql/v1beta4"
	gcpSQLMaxBodyBytes = 1 << 10
	gcpSQLMinSegments  = 3
	gcpSQLMaxSegments  = 5
	gcpSQLInstanceIdx  = 3
	gcpSQLSubIdx       = 4
)

var gcpSQLIDRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// registerGCPSQLRoutes exposes read-only Cloud SQL Admin lists (instances, databases,
// users) to the SPA by dispatching in-process to the emulator's handler.
func (h *DashboardHandler) registerGCPSQLRoutes() {
	var inner echo.HandlerFunc

	if h.config.GCPSQLOps != nil {
		inner = h.config.GCPSQLOps.Handler()
	}

	serve := newAzureProxyHandler(gcpSQLProxyPrefix, gcpSQLMaxBodyBytes, inner, gcpSQLProxyPath)
	h.SubRouter.Add(http.MethodGet, gcpSQLProxyPrefix+"/*", serve)
}

// gcpSQLProxyPath admits only /projects/<p>/instances[/<i>[/databases|users]].
func gcpSQLProxyPath(raw string) (string, bool) {
	segs := strings.Split(strings.TrimPrefix(raw, "/"), "/")
	if len(segs) < gcpSQLMinSegments || len(segs) > gcpSQLMaxSegments ||
		segs[0] != gcpRESTProjectsSeg || segs[2] != "instances" {
		return "", false
	}

	if !gcpSQLIDRe.MatchString(segs[1]) {
		return "", false
	}

	if len(segs) > gcpSQLInstanceIdx && !gcpSQLIDRe.MatchString(segs[gcpSQLInstanceIdx]) {
		return "", false
	}

	if len(segs) > gcpSQLSubIdx && segs[gcpSQLSubIdx] != "databases" && segs[gcpSQLSubIdx] != "users" {
		return "", false
	}

	return gcpSQLAPIPrefix + raw, true
}
