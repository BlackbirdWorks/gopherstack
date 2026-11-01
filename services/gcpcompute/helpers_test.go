package gcpcompute_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	compute "cloud.google.com/go/compute/apiv1"
	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/gcpcompute"
)

const (
	project = "test-project"
	zone    = "us-central1-a"
	region  = "us-central1"
)

type fakeClock struct {
	t  time.Time
	mu sync.Mutex
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.t = c.t.Add(d)
}

func (c *fakeClock) Sleep(_ context.Context, d time.Duration) { c.Advance(d) }

type env struct {
	backend *gcpcompute.InMemoryBackend
	clock   *fakeClock
	url     string
	opts    []option.ClientOption
}

func newEnv(t *testing.T) *env {
	t.Helper()

	backend := gcpcompute.NewInMemoryBackend()
	clock := &fakeClock{t: time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)}
	backend.SetClock(clock.Now, clock.Sleep)

	e := echo.New()
	e.Any("/*", gcpcompute.NewHandler(backend).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return &env{
		backend: backend, clock: clock, url: srv.URL,
		opts: []option.ClientOption{option.WithEndpoint(srv.URL), option.WithoutAuthentication()},
	}
}

func client[T interface{ Close() error }](
	t *testing.T,
	e *env,
	mk func(context.Context, ...option.ClientOption) (T, error),
) T {
	t.Helper()

	c, err := mk(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func (e *env) networks(t *testing.T) *compute.NetworksClient {
	t.Helper()

	return client(t, e, compute.NewNetworksRESTClient)
}

func (e *env) subnets(t *testing.T) *compute.SubnetworksClient {
	t.Helper()

	return client(t, e, compute.NewSubnetworksRESTClient)
}

func (e *env) firewalls(t *testing.T) *compute.FirewallsClient {
	t.Helper()

	return client(t, e, compute.NewFirewallsRESTClient)
}

func (e *env) routes(t *testing.T) *compute.RoutesClient {
	t.Helper()

	return client(t, e, compute.NewRoutesRESTClient)
}

func (e *env) addresses(t *testing.T) *compute.AddressesClient {
	t.Helper()

	return client(t, e, compute.NewAddressesRESTClient)
}

func (e *env) globalAddresses(t *testing.T) *compute.GlobalAddressesClient {
	t.Helper()

	return client(t, e, compute.NewGlobalAddressesRESTClient)
}

func (e *env) routers(t *testing.T) *compute.RoutersClient {
	t.Helper()

	return client(t, e, compute.NewRoutersRESTClient)
}

func (e *env) disks(t *testing.T) *compute.DisksClient {
	t.Helper()

	return client(t, e, compute.NewDisksRESTClient)
}

func (e *env) snapshots(t *testing.T) *compute.SnapshotsClient {
	t.Helper()

	return client(t, e, compute.NewSnapshotsRESTClient)
}

func (e *env) images(t *testing.T) *compute.ImagesClient {
	t.Helper()

	return client(t, e, compute.NewImagesRESTClient)
}

func (e *env) instances(t *testing.T) *compute.InstancesClient {
	t.Helper()

	return client(t, e, compute.NewInstancesRESTClient)
}

func (e *env) zoneOps(t *testing.T) *compute.ZoneOperationsClient {
	t.Helper()

	return client(t, e, compute.NewZoneOperationsRESTClient)
}

func (e *env) globalOps(t *testing.T) *compute.GlobalOperationsClient {
	t.Helper()

	return client(t, e, compute.NewGlobalOperationsRESTClient)
}

func (e *env) regionOps(t *testing.T) *compute.RegionOperationsClient {
	t.Helper()

	return client(t, e, compute.NewRegionOperationsRESTClient)
}

func (e *env) machineTypes(t *testing.T) *compute.MachineTypesClient {
	t.Helper()

	return client(t, e, compute.NewMachineTypesRESTClient)
}

func (e *env) zones(t *testing.T) *compute.ZonesClient {
	t.Helper()

	return client(t, e, compute.NewZonesRESTClient)
}

func (e *env) regions(t *testing.T) *compute.RegionsClient {
	t.Helper()

	return client(t, e, compute.NewRegionsRESTClient)
}

func (e *env) projects(t *testing.T) *compute.ProjectsClient {
	t.Helper()

	return client(t, e, compute.NewProjectsRESTClient)
}

// settle advances the fake clock past every operation and instance transition.
func (e *env) settle() { e.clock.Advance(time.Minute) }

func assertAPIError(t *testing.T, err error, status int, reason string) {
	t.Helper()

	require.Error(t, err)

	var ge *googleapi.Error

	require.ErrorAs(t, err, &ge)
	assert.Equal(t, status, ge.Code)

	if reason != "" {
		require.NotEmpty(t, ge.Errors)
		assert.Equal(t, reason, ge.Errors[0].Reason)
	}
}

func rawDo(t *testing.T, e *env, method, path, body string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, e.url+path, nil)
	if body != "" {
		req, err = http.NewRequestWithContext(t.Context(), method, e.url+path, stringsReader(body))
	}

	require.NoError(t, err)

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(data)
}

func s(v string) *string { return new(v) }

// done requires the call to succeed, lets the fake clock run past the operation and waits for it.
func (e *env) done(t *testing.T, op *compute.Operation, err error) {
	t.Helper()
	require.NoError(t, err)
	e.settle()
	require.NoError(t, op.Wait(t.Context()))
	assert.True(t, op.Done())
}

func (e *env) network(t *testing.T, name string, auto bool) {
	t.Helper()

	op, err := e.networks(t).Insert(t.Context(), &computepb.InsertNetworkRequest{
		Project: project, NetworkResource: &computepb.Network{Name: s(name), AutoCreateSubnetworks: new(auto)},
	})
	e.done(t, op, err)
}

func (e *env) subnet(t *testing.T, name, cidr string) {
	t.Helper()

	op, err := e.subnets(t).Insert(t.Context(), &computepb.InsertSubnetworkRequest{
		Project: project, Region: region, SubnetworkResource: &computepb.Subnetwork{
			Name: s(name), Network: s("projects/" + project + "/global/networks/vpc"), IpCidrRange: s(cidr),
		},
	})
	e.done(t, op, err)
}

func gcpcomputeTiming() gcpcompute.Timing { return gcpcompute.DefaultTiming() }
