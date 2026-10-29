package ocicontainerengine_test

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	ke "github.com/oracle/oci-go-sdk/v65/containerengine"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"

	oke "github.com/blackbirdworks/gopherstack/services/ocicontainerengine"
)

const (
	testToken = "unit-test-cluster-token-0123"
	testCA    = "-----BEGIN CERTIFICATE-----\nZmFrZQ==\n-----END CERTIFICATE-----\n"
	engineMax = 10 * time.Second
	engineGap = 5 * time.Millisecond
)

var errRuntimeDown = errors.New("runtime down")

type fakeRuntime struct {
	failErr error
	specs   []container.Spec
	removed []string
	mu      sync.Mutex
}

func (f *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.failErr != nil {
		return "", f.failErr
	}

	f.specs = append(f.specs, spec)

	return "ctr-" + spec.Name, nil
}

func (f *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *fakeRuntime) created() []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.specs...)
}

func (f *fakeRuntime) removedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.removed...)
}

type gate struct{ open chan struct{} }

func (g *gate) probe(ctx context.Context, _, _ string) ([]byte, error) {
	select {
	case <-g.open:
		return []byte(testCA), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type engineEnv struct {
	*env

	rt    *fakeRuntime
	gate  *gate
	ports *portalloc.Allocator
}

func newEngineEnv(t *testing.T, mod func(cfg *oke.EngineConfig, rt *fakeRuntime)) *engineEnv {
	t.Helper()

	ports, err := portalloc.New(42000, 42100)
	require.NoError(t, err)

	rt, g := &fakeRuntime{}, &gate{open: make(chan struct{})}
	cfg := oke.EngineConfig{Runtime: rt, Ports: ports, Probe: g.probe, Token: testToken, StartTimeout: engineMax}

	if mod != nil {
		mod(&cfg, rt)
	}

	return &engineEnv{
		env: newEnv(t, envOpts{engine: func(b *oke.InMemoryBackend) { require.NoError(t, b.EnableEngine(cfg)) }}),
		rt:  rt, gate: g, ports: ports,
	}
}

func (e *engineEnv) cluster(t *testing.T, id *string) ke.GetClusterResponse {
	t.Helper()

	got, err := e.ce.GetCluster(t.Context(), ke.GetClusterRequest{ClusterId: id})
	require.NoError(t, err)

	return got
}

func (e *engineEnv) create(t *testing.T, name string) (*string, *string) {
	t.Helper()

	resp, err := e.ce.CreateCluster(
		t.Context(),
		ke.CreateClusterRequest{CreateClusterDetails: clusterDetails(name)},
	)
	require.NoError(t, err)

	list, err := e.ce.ListClusters(
		t.Context(),
		ke.ListClustersRequest{CompartmentId: new(testCompartment), Name: new(name)},
	)
	require.NoError(t, err)
	require.Len(t, list.Items, 1)

	return list.Items[0].Id, resp.OpcWorkRequestId
}

func TestEngineClusterLifecycle(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, nil)
	id, wrID := e.create(t, "real")

	require.Eventually(t, func() bool { return len(e.rt.created()) == 1 }, engineMax, engineGap)

	spec := e.rt.created()[0]
	assert.True(t, strings.HasPrefix(spec.Image, k3sengine.ImageRepo+":v1.34."), spec.Image)
	assert.True(t, spec.Privileged)
	assert.Contains(t, spec.Env, "GOPHERSTACK_EKS_ADMIN_AUTH="+testToken)
	assert.Contains(t, spec.Name, "gopherstack-oke-")

	e.settle()
	assert.Equal(t, ke.ClusterLifecycleStateCreating, e.cluster(t, id).LifecycleState,
		"the cluster turns ACTIVE when the API server answers, not on the clock")

	wr, err := e.ce.GetWorkRequest(t.Context(), ke.GetWorkRequestRequest{WorkRequestId: wrID})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestStatusInProgress, wr.Status)

	close(e.gate.open)

	require.Eventually(t, func() bool {
		return e.cluster(t, id).LifecycleState == ke.ClusterLifecycleStateActive
	}, engineMax, engineGap)

	c := e.cluster(t, id).Cluster
	assert.Regexp(t, `^127\.0\.0\.1:42\d{3}$`, *c.Endpoints.PrivateEndpoint)
	assert.Equal(t, *c.Endpoints.PrivateEndpoint, *c.Endpoints.PublicEndpoint)
	assert.Equal(t, testToken, e.backend.EngineToken())

	wr, err = e.ce.GetWorkRequest(t.Context(), ke.GetWorkRequestRequest{WorkRequestId: wrID})
	require.NoError(t, err)
	assert.Equal(t, ke.WorkRequestStatusSucceeded, wr.Status)

	kc, err := e.ce.CreateKubeconfig(t.Context(), ke.CreateKubeconfigRequest{ClusterId: id})
	require.NoError(t, err)

	cfg := readKubeconfig(t, kc)
	assert.Equal(t, "https://"+*c.Endpoints.PublicEndpoint, cfg.Clusters[0].Cluster.Server)
	assert.Equal(t, testToken, cfg.Users[0].User.Token)
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(testCA)), cfg.Clusters[0].Cluster.CA)

	_, err = e.ce.DeleteCluster(t.Context(), ke.DeleteClusterRequest{ClusterId: id})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(e.rt.removedIDs()) == 1 }, engineMax, engineGap)
	assert.Equal(t, "ctr-"+spec.Name, e.rt.removedIDs()[0])
}

func TestEngineStartFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(cfg *oke.EngineConfig, rt *fakeRuntime)
		name string
		want string
	}{
		{
			name: "container start", want: "runtime down",
			mod: func(_ *oke.EngineConfig, rt *fakeRuntime) { rt.failErr = errRuntimeDown },
		},
		{
			name: "api server never ready", want: "API server not ready",
			mod: func(cfg *oke.EngineConfig, _ *fakeRuntime) {
				cfg.StartTimeout = 20 * time.Millisecond
				cfg.Probe = func(context.Context, string, string) ([]byte, error) { return nil, errRuntimeDown }
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEngineEnv(t, tt.mod)
			id, wrID := e.create(t, "bad")

			require.Eventually(t, func() bool {
				return e.cluster(t, id).LifecycleState == ke.ClusterLifecycleStateFailed
			}, engineMax, engineGap)

			c := e.cluster(t, id)
			assert.Contains(t, *c.LifecycleDetails, tt.want)

			wr, err := e.ce.GetWorkRequest(t.Context(), ke.GetWorkRequestRequest{WorkRequestId: wrID})
			require.NoError(t, err)
			assert.Equal(t, ke.WorkRequestStatusFailed, wr.Status)

			errs, err := e.ce.ListWorkRequestErrors(
				t.Context(),
				ke.ListWorkRequestErrorsRequest{CompartmentId: new(testCompartment), WorkRequestId: wrID},
			)
			require.NoError(t, err)
			require.Len(t, errs.Items, 1)
			assert.Contains(t, *errs.Items[0].Message, tt.want)

			_, err = e.ce.DeleteCluster(t.Context(), ke.DeleteClusterRequest{ClusterId: id})
			require.NoError(t, err, "FAILED clusters can be deleted")
		})
	}
}

func TestEngineRestoreRelaunches(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, nil)
	close(e.gate.open)

	id, _ := e.create(t, "again")

	require.Eventually(t, func() bool {
		return e.cluster(t, id).LifecycleState == ke.ClusterLifecycleStateActive
	}, engineMax, engineGap)

	snap := e.backend.Snapshot(t.Context())
	require.NoError(t, e.backend.Restore(t.Context(), snap))

	require.Eventually(t, func() bool { return len(e.rt.created()) == 2 }, engineMax, engineGap)
	require.Eventually(t, func() bool {
		c := e.cluster(t, id)

		return c.LifecycleState == ke.ClusterLifecycleStateActive && len(e.rt.removedIDs()) == 1
	}, engineMax, engineGap)
}

func TestEngineConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want error
		name string
		cfg  oke.EngineConfig
	}{
		{
			name: "bad host",
			cfg:  oke.EngineConfig{Host: "bad host!", Token: testToken},
			want: k3sengine.ErrInvalidConfig,
		},
		{name: "short token", cfg: oke.EngineConfig{Token: "short"}, want: k3sengine.ErrInvalidConfig},
		{
			name: "default token off host",
			cfg:  oke.EngineConfig{Host: "10.1.2.3"},
			want: k3sengine.ErrDefaultTokenExposed,
		},
		{name: "explicit token off host", cfg: oke.EngineConfig{Host: "10.1.2.3", Token: testToken}},
		{name: "loopback default token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.cfg.Runtime = &fakeRuntime{}
			b := oke.NewInMemoryBackend()
			t.Cleanup(b.Close)

			err := b.EnableEngine(tt.cfg)
			if tt.want == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.want)
		})
	}
}
