package dashboard

import (
	"bytes"
	"io"
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
	gcpComputeRevealParam  = "reveal"

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
	computeSnapshots    = "snapshots"
	computeSubnets      = "subnetworks"
	computeBackendSvcs  = "backendServices"
	computeURLMaps      = "urlMaps"
	computeHTTPProxies  = "targetHttpProxies"
	computeHTTPSProxies = "targetHttpsProxies"
	computeFwdRules     = "forwardingRules"
	computeStart        = "start"
	computeStop         = "stop"
	computeReset        = "reset"
	computeSuspend      = "suspend"
	computeResume       = "resume"
	computeSnapshotVerb = "createSnapshot"
)

//nolint:gochecknoglobals // read-only lookup tables
var (
	computeLB         = []string{computeBackendSvcs, computeURLMaps, computeHTTPProxies, computeHTTPSProxies, computeFwdRules}
	computeAggregated = append([]string{
		computeInstances, computeDisks, computeSubnets, computeAddresses, computeRouters, computeOperations,
	}, computeLB...)
	computeGlobal = append([]string{
		"networks", "firewalls", "routes", computeSnapshots, computeOperations, computeAddresses, "images",
		"securityPolicies",
	}, computeLB...)
	computeZonal           = []string{computeInstances, computeDisks, computeOperations, "machineTypes"}
	computeRegional        = append([]string{computeSubnets, computeAddresses, computeRouters, computeOperations}, computeLB...)
	computeInstanceActions = []string{computeStart, computeStop, computeReset, computeSuspend, computeResume}
	computeQueryKeys       = []string{
		"filter",
		"maxResults",
		"pageToken",
		"orderBy",
	}
)

// registerGCPComputeRoutes exposes Compute Engine browsing, instance and snapshot creation and resource management to
// the SPA by dispatching in-process to the emulator's handler. Metadata values never leave it unredacted.
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

		apiPath, parts, ok := gcpComputeProxyPath(r.Method, strings.TrimPrefix(r.URL.EscapedPath(), gcpComputeProxyPrefix))
		if !ok {
			return cloudJSONFail(
				c,
				http.StatusBadRequest,
				cloudStatusBadArg,
				"unsupported Compute Engine proxy request",
			)
		}

		body, err := gcpComputeBody(r.Method, parts, http.MaxBytesReader(c.Response(), r.Body, gcpComputeMaxBodyBytes))
		if err != nil {
			return cloudJSONFail(c, http.StatusBadRequest, cloudStatusBadArg, err.Error())
		}

		fwd := r.Clone(r.Context())
		fwd.URL.Path, fwd.URL.RawPath = unescapedPath(apiPath), apiPath
		fwd.URL.RawQuery = gcpComputeQuery(r.URL.Query())
		fwd.RequestURI = ""
		fwd.Body = io.NopCloser(bytes.NewReader(body))
		fwd.ContentLength = int64(len(body))

		if r.Method != http.MethodGet {
			inner.ServeHTTP(c.Response(), fwd)

			return nil
		}

		reveal := ""
		if len(parts) == computeScopedItem && parts[computeScopedColl] == computeInstances {
			reveal = r.URL.Query().Get(gcpComputeRevealParam)
		}

		return serveRedacted(c, inner, fwd, reveal)
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
// the compute, network, load balancing, Cloud Armor and operation collections; writes are limited to instance
// start/stop/reset/suspend/resume, creating an instance or a disk snapshot (bodies rebuilt from fixed fields), and
// deleting instances, disks, snapshots and addresses.
func gcpComputeProxyPath(method, raw string) (string, []string, bool) {
	parts := strings.Split(strings.TrimPrefix(raw, "/"), "/")
	if len(parts) < computeMinSegs || parts[0] != gcpRESTProjectsSeg {
		return "", nil, false
	}

	for _, p := range parts {
		dec, err := url.PathUnescape(p)
		if err != nil || dec == "" || dec == "." || dec == ".." || strings.ContainsAny(dec, "/:") {
			return "", nil, false
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
		return "", nil, false
	}

	return gcpComputeAPIPrefix + "/" + strings.Join(parts, "/"), parts, true
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
	if parts[computeScopeIdx] != computeScopeZones {
		return false
	}

	switch len(parts) {
	case computeScopedList:
		return parts[computeScopedColl] == computeInstances
	case computeScopedVerb:
		verb := parts[computeScopedVerb-1]

		return (parts[computeScopedColl] == computeInstances && slices.Contains(computeInstanceActions, verb)) ||
			(parts[computeScopedColl] == computeDisks && verb == computeSnapshotVerb)
	}

	return false
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
