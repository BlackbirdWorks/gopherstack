package dashboard

import (
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

const (
	gcpComputeProxyPrefix  = "/dashboard/api/v1/gcp/compute"
	gcpComputeAPIPrefix    = "/compute/v1"
	gcpComputeMaxBodyBytes = 1 << 20

	computeMinSegs      = 3
	computeScopeIdx     = 2
	computeGlobalList   = 4
	computeGlobalItem   = 5
	computeScopedList   = 5
	computeScopedItem   = 6
	computeScopedVerb   = 7
	computeScopedColl   = 4
	computeScopeAgg     = "aggregated"
	computeScopeGlobal  = "global"
	computeScopeZones   = "zones"
	computeScopeRegions = "regions"
	computeInstances    = "instances"
	computeOperations   = "operations"
	computeDisks        = "disks"
	computeAddresses    = "addresses"
	computeRouters      = "routers"
	computeStart        = "start"
	computeStop         = "stop"
	computeReset        = "reset"
)

//nolint:gochecknoglobals // read-only lookup tables
var (
	computeAggregated = []string{
		computeInstances, computeDisks, "subnetworks", computeAddresses, computeRouters, computeOperations,
	}
	computeGlobal = []string{
		"networks", "firewalls", "routes", "snapshots", computeOperations, computeAddresses,
	}
	computeZonal           = []string{computeInstances, computeDisks, computeOperations}
	computeRegional        = []string{"subnetworks", computeAddresses, computeRouters, computeOperations}
	computeInstanceActions = []string{computeStart, computeStop, computeReset}
	computeQueryKeys       = []string{
		"filter",
		"maxResults",
		"pageToken",
		"orderBy",
	}
)

// registerGCPComputeRoutes exposes Compute Engine browsing and instance, disk, snapshot and address
// management to the SPA by dispatching in-process to the emulator's handler.
func (h *DashboardHandler) registerGCPComputeRoutes() {
	var inner *echo.Echo

	if h.config.GCPComputeOps != nil {
		inner = echo.New()
		inner.Any("/*", h.config.GCPComputeOps.Handler())
	}

	serve := func(c *echo.Context) error {
		if inner == nil {
			return cloudUnavailable(c, "Compute Engine")
		}

		r := c.Request()

		apiPath, ok := gcpComputeProxyPath(r.Method, strings.TrimPrefix(r.URL.EscapedPath(), gcpComputeProxyPrefix))
		if !ok {
			return cloudJSONFail(
				c,
				http.StatusBadRequest,
				cloudStatusBadArg,
				"unsupported Compute Engine proxy request",
			)
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(apiPath), apiPath
		fwd.URL.RawQuery = gcpComputeQuery(r.URL.Query())
		fwd.RequestURI = ""
		fwd.Body = http.MaxBytesReader(c.Response(), r.Body, gcpComputeMaxBodyBytes)
		inner.ServeHTTP(c.Response(), fwd)

		return nil
	}

	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodDelete} {
		h.SubRouter.Add(method, gcpComputeProxyPrefix+"/*", serve)
	}
}

func gcpComputeQuery(in url.Values) string {
	out := url.Values{}

	for _, k := range computeQueryKeys {
		if v := in.Get(k); v != "" {
			out.Set(k, v)
		}
	}

	return out.Encode()
}

// gcpComputeProxyPath maps /projects/<p>/... below the proxy prefix to the emulator's /compute/v1 path. Reads cover
// instances, disks, snapshots, networks, subnetworks, firewalls, routes, routers, addresses and operations; writes are
// limited to instance start/stop/reset and deleting instances, disks, snapshots and addresses.
func gcpComputeProxyPath(method, raw string) (string, bool) {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")
	if len(parts) < computeMinSegs || parts[0] != gcpRESTProjectsSeg {
		return "", false
	}

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.ContainsAny(dec, "/:") {
			return "", false
		}
	}

	var allowed bool

	switch method {
	case http.MethodGet:
		allowed = computeReadable(parts)
	case http.MethodPost:
		allowed = computeActionable(parts)
	case http.MethodDelete:
		allowed = computeDeletable(parts)
	}

	if !allowed {
		return "", false
	}

	return gcpComputeAPIPrefix + "/" + strings.Join(parts, "/"), true
}

func computeReadable(parts []string) bool {
	n := len(parts)

	switch parts[computeScopeIdx] {
	case computeScopeAgg:
		return n == computeGlobalList && slices.Contains(computeAggregated, parts[computeGlobalList-1])
	case computeScopeGlobal:
		return (n == computeGlobalList || n == computeGlobalItem) &&
			slices.Contains(computeGlobal, parts[computeGlobalList-1])
	case computeScopeZones:
		return n <= computeGlobalList || computeScopedRead(parts, computeZonal)
	case computeScopeRegions:
		if n == computeScopedVerb && parts[computeScopedColl] == "routers" {
			return parts[computeScopedVerb-1] == "getRouterStatus"
		}

		return n <= computeGlobalList || computeScopedRead(parts, computeRegional)
	}

	return false
}

func computeScopedRead(parts, collections []string) bool {
	n := len(parts)

	return (n == computeScopedList || n == computeScopedItem) && slices.Contains(collections, parts[computeScopedColl])
}

func computeActionable(parts []string) bool {
	return len(parts) == computeScopedVerb && parts[computeScopeIdx] == computeScopeZones &&
		parts[computeScopedColl] == computeInstances && slices.Contains(computeInstanceActions, parts[computeScopedVerb-1])
}

func computeDeletable(parts []string) bool {
	n := len(parts)

	switch parts[computeScopeIdx] {
	case computeScopeZones:
		return n == computeScopedItem &&
			(parts[computeScopedColl] == computeInstances || parts[computeScopedColl] == computeDisks)
	case computeScopeRegions:
		return n == computeScopedItem && parts[computeScopedColl] == computeAddresses
	case computeScopeGlobal:
		return n == computeGlobalItem &&
			(parts[computeGlobalList-1] == "snapshots" || parts[computeGlobalList-1] == computeAddresses)
	}

	return false
}
