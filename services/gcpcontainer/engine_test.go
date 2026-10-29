//nolint:staticcheck // the API still serves fields the proto marks deprecated
package gcpcontainer_test

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/container/apiv1/containerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/services/gcpcontainer"
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

type gate struct {
	err  error
	open chan struct{}
	mu   sync.Mutex
}

func newGate() *gate { return &gate{open: make(chan struct{})} }

func (g *gate) release() { close(g.open) }

func (g *gate) probe(ctx context.Context, _, _ string) ([]byte, error) {
	g.mu.Lock()
	err := g.err
	g.mu.Unlock()

	if err != nil {
		return nil, err
	}

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

func newEngineEnv(t *testing.T, mod func(cfg *gcpcontainer.EngineConfig, rt *fakeRuntime)) *engineEnv {
	t.Helper()

	ports, err := portalloc.New(41000, 41100)
	require.NoError(t, err)

	rt, g := &fakeRuntime{}, newGate()
	cfg := gcpcontainer.EngineConfig{
		Runtime:      rt,
		Ports:        ports,
		Probe:        g.probe,
		Token:        testToken,
		StartTimeout: engineMax,
	}

	if mod != nil {
		mod(&cfg, rt)
	}

	b := gcpcontainer.NewInMemoryBackend()
	require.NoError(t, b.EnableEngine(cfg))

	return &engineEnv{env: newEnvWith(t, b), rt: rt, gate: g, ports: ports}
}

func (e *engineEnv) opDone(t *testing.T, op *containerpb.Operation) *containerpb.Operation {
	t.Helper()

	var got *containerpb.Operation

	require.Eventually(t, func() bool {
		got = e.getOp(t, op)

		return got.GetStatus() == containerpb.Operation_DONE
	}, engineMax, engineGap)

	return got
}

func TestEngineClusterLifecycle(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, nil)
	op := e.create(
		t,
		zone,
		&containerpb.Cluster{Name: "real", InitialNodeCount: 1, InitialClusterVersion: "1.34.2-gke.1008000"},
	)

	require.Eventually(t, func() bool { return len(e.rt.created()) == 1 }, engineMax, engineGap)

	spec := e.rt.created()[0]
	assert.True(t, strings.HasPrefix(spec.Image, k3sengine.ImageRepo+":v1.34."), spec.Image)
	assert.True(t, spec.Privileged)
	assert.Contains(t, spec.Env, "GOPHERSTACK_EKS_ADMIN_AUTH="+testToken)
	assert.Contains(t, spec.Name, "gopherstack-gke-real-")
	require.Len(t, spec.Ports, 1)

	e.settle()
	assert.Equal(
		t,
		containerpb.Operation_RUNNING,
		e.getOp(t, op).GetStatus(),
		"the operation ends when the API server answers, not on the clock",
	)
	assert.Equal(t, containerpb.Cluster_PROVISIONING, e.getCluster(t, zone, "real").GetStatus())

	e.gate.release()
	done := e.opDone(t, op)
	assert.Nil(t, done.GetError())

	c := e.getCluster(t, zone, "real")
	assert.Equal(t, containerpb.Cluster_RUNNING, c.GetStatus())
	assert.Regexp(t, `^127\.0\.0\.1:41\d{3}$`, c.GetEndpoint())
	assert.Equal(t, base64.StdEncoding.EncodeToString([]byte(testCA)), c.GetMasterAuth().GetClusterCaCertificate())
	assert.Equal(t, testToken, e.b.EngineToken())

	port := strings.TrimPrefix(c.GetEndpoint(), "127.0.0.1:")
	assert.Contains(t, spec.Ports[0], port)

	_, err := e.gc.DeleteCluster(t.Context(), &containerpb.DeleteClusterRequest{Name: clusterPath(zone, "real")})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(e.rt.removedIDs()) == 1 }, engineMax, engineGap)
	assert.Equal(t, "ctr-"+spec.Name, e.rt.removedIDs()[0])
	require.Eventually(t, func() bool { return !e.ports.IsAllocated(portOf(t, c.GetEndpoint())) }, engineMax, engineGap)
}

func portOf(t *testing.T, endpoint string) int {
	t.Helper()

	_, p, ok := strings.Cut(endpoint, ":")
	require.True(t, ok)

	var n int

	for _, ch := range p {
		n = n*10 + int(ch-'0')
	}

	return n
}

func TestEngineDeleteWhileProvisioning(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, nil)
	create := e.create(t, zone, stdCluster("early"))

	require.Eventually(t, func() bool { return len(e.rt.created()) == 1 }, engineMax, engineGap)

	_, err := e.gc.DeleteCluster(t.Context(), &containerpb.DeleteClusterRequest{Name: clusterPath(zone, "early")})
	require.NoError(t, err)
	assert.Equal(t, containerpb.Operation_DONE, e.getOp(t, create).GetStatus(), "the pending create is aborted")

	require.Eventually(t, func() bool { return len(e.rt.removedIDs()) == 1 }, engineMax, engineGap)
}

func TestEngineStartFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mod  func(cfg *gcpcontainer.EngineConfig, rt *fakeRuntime)
		name string
		want string
	}{
		{
			name: "container start", want: "runtime down",
			mod: func(_ *gcpcontainer.EngineConfig, rt *fakeRuntime) { rt.failErr = errRuntimeDown },
		},
		{
			name: "api server never ready", want: "API server not ready",
			mod: func(cfg *gcpcontainer.EngineConfig, _ *fakeRuntime) {
				cfg.StartTimeout = 20 * time.Millisecond
				cfg.Probe = func(context.Context, string, string) ([]byte, error) { return nil, errRuntimeDown }
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEngineEnv(t, tt.mod)
			op := e.create(t, zone, stdCluster("bad"))

			done := e.opDone(t, op)
			assert.Contains(t, done.GetStatusMessage(), tt.want)
			assert.Equal(t, int32(codes.Internal), done.GetError().GetCode())

			c := e.getCluster(t, zone, "bad")
			assert.Equal(t, containerpb.Cluster_ERROR, c.GetStatus())
			assert.Contains(t, c.GetStatusMessage(), tt.want)
			assert.Equal(t, containerpb.NodePool_ERROR, c.GetNodePools()[0].GetStatus())

			_, err := e.gc.DeleteCluster(t.Context(), &containerpb.DeleteClusterRequest{Name: clusterPath(zone, "bad")})
			require.NoError(t, err)
		})
	}
}

func TestEngineRejectsIPRotation(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(t, nil)
	e.gate.release()
	op := e.create(t, zone, stdCluster("rot"))
	e.opDone(t, op)

	_, err := e.gc.StartIPRotation(t.Context(), &containerpb.StartIPRotationRequest{Name: clusterPath(zone, "rot")})
	assertCode(t, codes.FailedPrecondition, err, "docker engine")
}

func TestEngineConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want error
		name string
		cfg  gcpcontainer.EngineConfig
	}{
		{
			name: "bad host",
			cfg:  gcpcontainer.EngineConfig{Host: "bad host!", Token: testToken},
			want: k3sengine.ErrInvalidConfig,
		},
		{name: "short token", cfg: gcpcontainer.EngineConfig{Token: "short"}, want: k3sengine.ErrInvalidConfig},
		{
			name: "token charset",
			cfg:  gcpcontainer.EngineConfig{Token: "has spaces in it......"},
			want: k3sengine.ErrInvalidConfig,
		},
		{
			name: "default token off host",
			cfg:  gcpcontainer.EngineConfig{Host: "10.1.2.3"},
			want: k3sengine.ErrDefaultTokenExposed,
		},
		{name: "explicit token off host", cfg: gcpcontainer.EngineConfig{Host: "10.1.2.3", Token: testToken}},
		{name: "loopback default token"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.cfg.Runtime = &fakeRuntime{}
			b := gcpcontainer.NewInMemoryBackend()
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

func TestEngineImageOverride(t *testing.T) {
	t.Parallel()

	e := newEngineEnv(
		t,
		func(cfg *gcpcontainer.EngineConfig, _ *fakeRuntime) { cfg.Image = "registry.test/k3s:custom" },
	)
	e.create(t, zone, stdCluster("img"))

	require.Eventually(t, func() bool { return len(e.rt.created()) == 1 }, engineMax, engineGap)
	assert.Equal(t, "registry.test/k3s:custom", e.rt.created()[0].Image)
}

func TestEngineRestoreRelaunches(t *testing.T) {
	t.Parallel()

	src := newEngineEnv(t, nil)
	src.gate.release()
	src.opDone(t, src.create(t, zone, stdCluster("again")))

	data := src.h.Snapshot(t.Context())

	dst := newEngineEnv(t, nil)
	dst.clock.Advance(src.clock.Now().Sub(dst.clock.Now()))
	require.NoError(t, dst.h.Restore(t.Context(), data))

	require.Eventually(t, func() bool { return len(dst.rt.created()) == 1 }, engineMax, engineGap)
	assert.Equal(t, containerpb.Cluster_PROVISIONING, dst.getCluster(t, zone, "again").GetStatus())

	dst.gate.release()
	require.Eventually(t, func() bool {
		return dst.getCluster(t, zone, "again").GetStatus() == containerpb.Cluster_RUNNING
	}, engineMax, engineGap)
}
