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
			`"metadata":{"items":[{"key":"startup-script","value":"echo secret-ish"}]},"disks":[{"boot":true,` +
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
		}},
		{"actions", []gcpStep{
			{http.MethodPost, vm + "/stop", "", `"operationType":"stop"`, 200},
			{http.MethodGet, vm, "", "TERMINATED", 200},
			{http.MethodPost, vm + "/start", "", `"operationType":"start"`, 200},
			{http.MethodPost, vm + "/reset", "", `"operationType":"reset"`, 200},
			{http.MethodDelete, root + "/regions/us-central1/addresses/addr1", "", `"operationType":"delete"`, 200},
			{http.MethodDelete, vm, "", `"operationType":"delete"`, 200},
			{http.MethodGet, vm, "", "notFound", 404},
		}},
		{"rejects", []gcpStep{
			{http.MethodPost, zone + "/instances", `{"name":"x"}`, "unsupported", 400},
			{http.MethodPost, vm + "/setMetadata", "{}", "unsupported", 400},
			{http.MethodPost, vm + "/stop%3Fx", "", "unsupported", 400},
			{http.MethodPut, vm, "{}", "Method Not Allowed", 405},
			{http.MethodDelete, root + "/global/networks/vpc", "", "unsupported", 400},
			{http.MethodGet, root + "/global/images", "", "unsupported", 400},
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
