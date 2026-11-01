package dashboard_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/gcpcompute"
)

type stepClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *stepClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(time.Minute)

	return c.t
}

func (c *stepClock) sleep(context.Context, time.Duration) {}

func seedComputeHandler(t *testing.T) *gcpcompute.Handler {
	t.Helper()

	const (
		api = "/compute/v1/projects/p"
		net = `{"name":"vpc","autoCreateSubnetworks":false}`
		sub = `{"name":"sub","network":"global/networks/vpc","ipCidrRange":"10.10.0.0/24"}`
		vm  = `{"name":"vm1","machineType":"zones/us-central1-a/machineTypes/e2-medium","labels":{"env":"dev"},` +
			`"metadata":{"items":[{"key":"startup-script","value":"echo secret-ish"},` +
			`{"key":"ssh-keys","value":"alice:ssh-rsa AAAAprivate"}]},"disks":[{"boot":true,` +
			`"autoDelete":true,"initializeParams":{"sourceImage":"projects/debian-cloud/global/images/family/debian-12",` +
			`"diskSizeGb":"10"}}],"networkInterfaces":[{"subnetwork":"regions/us-central1/subnetworks/sub",` +
			`"accessConfigs":[{"name":"External NAT","type":"ONE_TO_ONE_NAT"}]}]}`
	)

	backend := gcpcompute.NewInMemoryBackend()
	clock := &stepClock{t: time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)}
	backend.SetClock(clock.now, clock.sleep)

	ch := gcpcompute.NewHandler(backend)
	e := echo.New()
	e.Any("/*", ch.Handler())

	for _, s := range []struct{ path, body string }{
		{api + "/global/networks", net},
		{api + "/regions/us-central1/subnetworks", sub},
		{api + "/zones/us-central1-a/instances", vm},
		{api + "/regions/us-central1/addresses", `{"name":"addr1"}`},
	} {
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, s.path, strings.NewReader(s.body)))
		require.Equal(t, http.StatusOK, rec.Code, s.path+": "+rec.Body.String())
	}

	return ch
}

func TestGCPComputeProxy(t *testing.T) {
	t.Parallel()

	const (
		root = "/dashboard/api/v1/gcp/compute/projects/p"
		zone = root + "/zones/us-central1-a"
		vm   = zone + "/instances/vm1"
	)

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"browse", []gcpStep{
			{http.MethodGet, root + "/zones", "", "us-central1-a", 200},
			{http.MethodGet, root + "/regions", "", "us-central1", 200},
			{http.MethodGet, root + "/aggregated/instances", "", `"vm1"`, 200},
			{http.MethodGet, zone + "/instances", "", `"env":"dev"`, 200},
			{http.MethodGet, vm, "", "startup-script", 200},
			{http.MethodGet, zone + "/instances?maxResults=1&filter=name%3Dvm1", "", `"vm1"`, 200},
			{http.MethodGet, root + "/aggregated/disks", "", "vm1", 200},
			{http.MethodGet, root + "/aggregated/subnetworks", "", "10.10.0.0/24", 200},
			{http.MethodGet, root + "/aggregated/addresses", "", "addr1", 200},
			{http.MethodGet, root + "/aggregated/routers", "", "routers", 200},
			{http.MethodGet, root + "/global/networks", "", `"vpc"`, 200},
			{http.MethodGet, root + "/global/firewalls", "", "firewallList", 200},
			{http.MethodGet, root + "/global/routes", "", "routeList", 200},
			{http.MethodGet, root + "/global/snapshots", "", "snapshotList", 200},
			{http.MethodGet, root + "/aggregated/operations", "", "operation", 200},
			{http.MethodGet, root + "/regions/us-central1/addresses/addr1", "", "addr1", 200},
			{http.MethodGet, zone + "/machineTypes", "", "e2-medium", 200},
			{http.MethodGet, root + "/global/images", "", "imageList", 200},
			{http.MethodGet, root + "/global/securityPolicies", "", "securityPolicyList", 200},
			{http.MethodGet, root + "/global/backendServices", "", "backendServiceList", 200},
			{http.MethodGet, root + "/aggregated/backendServices", "", "AggregatedList", 200},
			{http.MethodGet, root + "/aggregated/forwardingRules", "", "AggregatedList", 200},
			{http.MethodGet, root + "/aggregated/urlMaps", "", "AggregatedList", 200},
			{http.MethodGet, root + "/aggregated/targetHttpProxies", "", "AggregatedList", 200},
			{http.MethodGet, root + "/global/forwardingRules", "", "forwardingRuleList", 200},
			{http.MethodGet, root + "/regions/us-central1/urlMaps", "", "urlMapList", 200},
			{http.MethodGet, root + "/global/operations", "", "operationList", 200},
			{http.MethodGet, zone + "/operations", "", "operationList", 200},
		}},
		{"actions", []gcpStep{
			{http.MethodPost, vm + "/stop", "", `"operationType":"stop"`, 200},
			{http.MethodGet, vm, "", "TERMINATED", 200},
			{http.MethodPost, vm + "/start", "", `"operationType":"start"`, 200},
			{http.MethodPost, vm + "/reset", "", `"operationType":"reset"`, 200},
			{http.MethodPost, vm + "/suspend", "", `"operationType":"suspend"`, 200},
			{http.MethodPost, vm + "/resume", "", `"operationType":"resume"`, 200},
			{http.MethodDelete, root + "/regions/us-central1/addresses/addr1", "", `"operationType":"delete"`, 200},
			{http.MethodDelete, vm, "", `"operationType":"delete"`, 200},
			{http.MethodGet, vm, "", "notFound", 404},
		}},
		{"rejects", []gcpStep{
			{http.MethodPost, zone + "/instances", `{"name":"x"}`, "machine type are required", 400},
			{http.MethodPost, zone + "/instances", `not json`, "invalid request body", 400},
			{http.MethodPost, zone + "/disks", `{}`, "unsupported", 400},
			{http.MethodPost, zone + "/disks/vm1/resize", `{}`, "unsupported", 400},
			{http.MethodPost, root + "/global/networks", `{"name":"x"}`, "unsupported", 400},
			{http.MethodPost, root + "/global/firewalls", `{"name":"x"}`, "unsupported", 400},
			{http.MethodDelete, root + "/global/firewalls/x", "", "unsupported", 400},
			{http.MethodGet, root + "/global/instanceTemplates", "", "unsupported", 400},
			{http.MethodGet, root + "/projects", "", "unsupported", 400},
			{http.MethodPost, vm + "/setMetadata", "{}", "unsupported", 400},
			{http.MethodPost, vm + "/stop%3Fx", "", "unsupported", 400},
			{http.MethodPut, vm, "{}", "Method Not Allowed", 405},
			{http.MethodDelete, root + "/global/networks/vpc", "", "unsupported", 400},
			{http.MethodGet, root + "/global/sslCertificates", "", "unsupported", 400},
			{http.MethodGet, root + "/zones/%2e%2e/instances", "", "unsupported", 400},
			{http.MethodGet, root + "/global/networks/vpc/extra", "", "unsupported", 400},
			{http.MethodGet, "/dashboard/api/v1/gcp/compute/other/p/zones", "", "unsupported", 400},
			{http.MethodGet, zone + "/instances/nope", "", "notFound", 404},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{GCPComputeOps: seedComputeHandler(t)})
			runGCPSteps(t, h, tt.steps)
		})
	}
}

func TestGCPComputeProxyUnserved(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/gcp/compute/projects/p/zones", nil)

	h := dashboard.NewHandler(dashboard.Config{})
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
	assert.Contains(t, rec.Body.String(), "not enabled")
}

func TestGCPComputeProxyMetadataRedaction(t *testing.T) {
	t.Parallel()

	const (
		zone = "/dashboard/api/v1/gcp/compute/projects/p/zones/us-central1-a"
		vm   = zone + "/instances/vm1"
	)

	get := func(t *testing.T, h *dashboard.DashboardHandler, path string) string {
		t.Helper()

		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

		return rec.Body.String()
	}

	tests := []struct {
		name       string
		path       string
		wantValue  []string
		wantAbsent []string
	}{
		{"item default", vm, []string{`"startup-script"`, `"valueLength":15`, `"redacted":true`},
			[]string{"secret-ish", "AAAAprivate"}},
		{"zonal list", zone + "/instances", []string{`"valueLength":15`}, []string{"secret-ish", "AAAAprivate"}},
		{"aggregated list", "/dashboard/api/v1/gcp/compute/projects/p/aggregated/instances",
			[]string{`"valueLength":15`}, []string{"secret-ish", "AAAAprivate"}},
		{"reveal startup script", vm + "?reveal=startup-script", []string{"echo secret-ish"}, []string{"AAAAprivate"}},
		{"reveal ssh keys refused", vm + "?reveal=ssh-keys", []string{`"redacted":true`}, []string{"AAAAprivate"}},
		{"reveal ignored on list", zone + "/instances?reveal=startup-script", []string{`"redacted":true`},
			[]string{"secret-ish"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{GCPComputeOps: seedComputeHandler(t)})
			body := get(t, h, tt.path)

			for _, w := range tt.wantValue {
				assert.Contains(t, body, w)
			}

			for _, a := range tt.wantAbsent {
				assert.NotContains(t, body, a)
			}
		})
	}
}

func TestGCPComputeProxyCreate(t *testing.T) {
	t.Parallel()

	const (
		root = "/dashboard/api/v1/gcp/compute/projects/p"
		zone = root + "/zones/us-central1-a"
		img  = "projects/debian-cloud/global/images/family/debian-12"
		good = `{"name":"web","machineType":"e2-small","labels":{"env":"dev"},` +
			`"metadata":{"items":[{"key":"startup-script","value":"leak-me"}]},` +
			`"serviceAccounts":[{"email":"x@y.z","scopes":["a"]}],` +
			`"disks":[{"boot":true,"initializeParams":{"sourceImage":"` + img + `","diskSizeGb":"12"}}],` +
			`"networkInterfaces":[{"network":"global/networks/vpc","subnetwork":"regions/us-central1/subnetworks/sub",` +
			`"accessConfigs":[{"type":"ONE_TO_ONE_NAT","natIP":"1.2.3.4"}]}]}`
	)

	tests := []struct {
		name  string
		steps []gcpStep
	}{
		{"rebuilds body", []gcpStep{
			{http.MethodPost, zone + "/instances", good, `"operationType":"insert"`, 200},
			{http.MethodGet, zone + "/instances/web", "", "zones/us-central1-a/machineTypes/e2-small", 200},
			{http.MethodGet, zone + "/instances/web", "", `"env":"dev"`, 200},
		}},
		{"snapshot", []gcpStep{
			{http.MethodPost, zone + "/disks/vm1/createSnapshot", `{"name":"snap1","labels":{"a":"b"},"storageLocations":["x"]}`,
				`"operationType":"createSnapshot"`, 200},
			{http.MethodGet, root + "/global/snapshots/snap1", "", `"sourceDisk"`, 200},
			{http.MethodPost, zone + "/disks/vm1/createSnapshot", `{"name":"Bad_Name"}`, "snapshot name", 400},
			{http.MethodPost, zone + "/disks/vm1/createSnapshot", `{"name":"snap1"}`, "already exists", 409},
			{http.MethodDelete, root + "/global/snapshots/snap1", "", `"operationType":"delete"`, 200},
		}},
		{"rejects bad fields", []gcpStep{
			{http.MethodPost, zone + "/instances", `{"name":"Web","machineType":"e2-small"}`, "required", 400},
			{http.MethodPost, zone + "/instances", `{"name":"web","machineType":"e2-small","disks":[],"networkInterfaces":[]}`,
				"exactly one", 400},
			{http.MethodPost, zone + "/instances", `{"name":"web","machineType":"e2-small",` +
				`"disks":[{"initializeParams":{"sourceImage":"https://evil/x"}}],"networkInterfaces":[{}]}`, "boot image", 400},
			{http.MethodPost, zone + "/instances", `{"name":"web","machineType":"e2-small",` +
				`"disks":[{"initializeParams":{"sourceImage":"` + img + `","diskSizeGb":"0"}}],"networkInterfaces":[{}]}`,
				"disk size", 400},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := dashboard.NewHandler(dashboard.Config{GCPComputeOps: seedComputeHandler(t)})
			runGCPSteps(t, h, tt.steps)
		})
	}
}

func TestGCPComputeProxyCreateDropsUnlistedFields(t *testing.T) {
	t.Parallel()

	const zone = "/dashboard/api/v1/gcp/compute/projects/p/zones/us-central1-a"

	body := `{"name":"web","machineType":"e2-small","metadata":{"items":[{"key":"startup-script","value":"leak-me"}]},` +
		`"serviceAccounts":[{"email":"x@y.z","scopes":["a"]}],"tags":{"items":["t"]},` +
		`"disks":[{"initializeParams":{"sourceImage":"projects/debian-cloud/global/images/family/debian-12"}}],` +
		`"networkInterfaces":[{"network":"vpc","subnetwork":"sub"}]}`

	h := dashboard.NewHandler(dashboard.Config{GCPComputeOps: seedComputeHandler(t)})
	runGCPSteps(t, h, []gcpStep{{http.MethodPost, zone + "/instances", body, "insert", 200}})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, zone+"/instances/web?reveal=startup-script", nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "leak-me")
	assert.NotContains(t, rec.Body.String(), "x@y.z")
	assert.NotContains(t, rec.Body.String(), `"items":["t"]`)
}
