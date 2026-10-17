package neptune_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

var errRuntimeDown = errors.New("runtime down")

type fakeEngineRuntime struct {
	createErr error
	specs     []container.Spec
	removed   []string
	stopped   []string
	started   []string
	mu        sync.Mutex
}

func (f *fakeEngineRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.createErr != nil {
		return "", f.createErr
	}

	f.specs = append(f.specs, spec)

	return "ctr-" + spec.Name, nil
}

func (f *fakeEngineRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *fakeEngineRuntime) StopContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stopped = append(f.stopped, id)

	return nil
}

func (f *fakeEngineRuntime) StartContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.started = append(f.started, id)

	return nil
}

func (f *fakeEngineRuntime) read(pick func(*fakeEngineRuntime) []string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), pick(f)...)
}

func (f *fakeEngineRuntime) removedIDs() []string {
	return f.read(func(r *fakeEngineRuntime) []string { return r.removed })
}

func (f *fakeEngineRuntime) stoppedIDs() []string {
	return f.read(func(r *fakeEngineRuntime) []string { return r.stopped })
}

func (f *fakeEngineRuntime) startedIDs() []string {
	return f.read(func(r *fakeEngineRuntime) []string { return r.started })
}

func (f *fakeEngineRuntime) createdSpecs() []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.specs...)
}

type gatedProbe struct{ gate chan struct{} }

func newGatedProbe() *gatedProbe { return &gatedProbe{gate: make(chan struct{})} }

func (g *gatedProbe) probe(ctx context.Context, _ string) error {
	select {
	case <-g.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func readyProbe(context.Context, string) error { return nil }

func engineBackend(
	t *testing.T, rt *fakeEngineRuntime, probe func(context.Context, string) error,
) *neptune.InMemoryBackend {
	t.Helper()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b.EnableEngine(neptune.EngineConfig{Runtime: rt, Probe: probe, StartTimeout: time.Minute})
	t.Cleanup(b.Close)

	return b
}

func createEngineCluster(t *testing.T, b *neptune.InMemoryBackend, version string) *neptune.DBCluster {
	t.Helper()

	c, err := b.CreateDBCluster(t.Context(), "c1", "", 0, neptune.DBClusterCreateOptions{EngineVersion: version})
	require.NoError(t, err)

	return c
}

func describeCluster(t *testing.T, b *neptune.InMemoryBackend, id string) neptune.DBCluster {
	t.Helper()

	cs, err := b.DescribeDBClusters(t.Context(), id, neptune.DBClusterFilters{})
	require.NoError(t, err)
	require.Len(t, cs, 1)

	return cs[0]
}

func waitCluster(t *testing.T, b *neptune.InMemoryBackend, id, want string) neptune.DBCluster {
	t.Helper()

	var c neptune.DBCluster

	require.Eventually(t, func() bool {
		cs, err := b.DescribeDBClusters(t.Context(), id, neptune.DBClusterFilters{})
		if err != nil || len(cs) != 1 {
			return false
		}

		c = cs[0]

		return c.Status == want
	}, 10*time.Second, 5*time.Millisecond)

	return c
}

func describeInstance(t *testing.T, b *neptune.InMemoryBackend) neptune.DBInstance {
	t.Helper()

	is, err := b.DescribeDBInstances(t.Context(), "i1", neptune.DBInstanceFilters{})
	require.NoError(t, err)
	require.Len(t, is, 1)

	return is[0]
}

func publishedPort(t *testing.T, spec container.Spec) int {
	t.Helper()

	require.Len(t, spec.Ports, 1)
	parts := strings.Split(spec.Ports[0], ":")
	require.Len(t, parts, 3)
	assert.Equal(t, "8182", parts[2])

	p, err := strconv.Atoi(parts[1])
	require.NoError(t, err)

	return p
}

func TestEngineCreateClusterStartsGremlin(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		version   string
		wantImage string
	}{
		{name: "neptune10", version: "1.0.5.1", wantImage: "tinkerpop/gremlin-server:3.4.13"},
		{name: "neptune11", version: "1.1.0.0", wantImage: "tinkerpop/gremlin-server:3.4.13"},
		{name: "neptune12", version: "1.2.1.0", wantImage: "tinkerpop/gremlin-server:3.5.7"},
		{name: "neptune13", version: "1.3.0.0", wantImage: "tinkerpop/gremlin-server:3.6.8"},
		{name: "neptune14", version: "1.4.1.0", wantImage: "tinkerpop/gremlin-server:3.7.3"},
		{name: "default", version: "", wantImage: "tinkerpop/gremlin-server:3.6.8"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeEngineRuntime{}
			gate := newGatedProbe()
			b := engineBackend(t, rt, gate.probe)

			created := createEngineCluster(t, b, tt.version)
			assert.Equal(t, "creating", created.Status)
			assert.Contains(t, created.Endpoint, "neptune.amazonaws.com")

			_, err := b.CreateDBInstance(t.Context(), "i1", "c1", "", neptune.DBInstanceCreateOptions{})
			require.NoError(t, err)
			assert.Equal(t, "creating", describeInstance(t, b).DBInstanceStatus)

			close(gate.gate)
			c := waitCluster(t, b, "c1", "available")

			specs := rt.createdSpecs()
			require.Len(t, specs, 1)
			assert.Equal(t, tt.wantImage, specs[0].Image)
			assert.True(t, strings.HasPrefix(specs[0].Name, "gopherstack-neptune-"))

			port := publishedPort(t, specs[0])
			assert.Equal(t, "127.0.0.1", c.Endpoint)
			assert.Equal(t, "127.0.0.1", c.ReaderEndpoint)
			assert.Equal(t, port, c.Port)

			require.Eventually(t, func() bool {
				return describeInstance(t, b).DBInstanceStatus == "available"
			}, 10*time.Second, 5*time.Millisecond)

			inst := describeInstance(t, b)
			assert.Equal(t, "127.0.0.1", inst.Endpoint)
			assert.Equal(t, port, inst.Port)
		})
	}
}

func TestEngineStubModeUnchanged(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	c := createEngineCluster(t, b, "1.3.0.0")

	assert.Equal(t, "available", c.Status)
	assert.Equal(t, "c1.cluster.us-east-1.neptune.amazonaws.com", c.Endpoint)
	assert.Equal(t, 8182, c.Port)

	_, err := b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	assert.Equal(t, "stopped", describeCluster(t, b, "c1").Status)
}

func TestEngineInstanceJoinsReadyCluster(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, readyProbe)
	createEngineCluster(t, b, "1.3.0.0")
	c := waitCluster(t, b, "c1", "available")

	inst, err := b.CreateDBInstance(t.Context(), "i1", "c1", "", neptune.DBInstanceCreateOptions{})
	require.NoError(t, err)

	assert.Equal(t, "available", inst.DBInstanceStatus)
	assert.Equal(t, "127.0.0.1", inst.Endpoint)
	assert.Equal(t, c.Port, inst.Port)
	assert.Len(t, rt.createdSpecs(), 1)
}

func TestEngineStartFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		rt    *fakeEngineRuntime
		probe func(context.Context, string) error
		name  string
	}{
		{name: "create_error", rt: &fakeEngineRuntime{createErr: errRuntimeDown}, probe: readyProbe},
		{
			name:  "never_ready",
			rt:    &fakeEngineRuntime{},
			probe: func(context.Context, string) error { return errRuntimeDown },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
			b.EnableEngine(neptune.EngineConfig{Runtime: tt.rt, Probe: tt.probe, StartTimeout: 50 * time.Millisecond})
			t.Cleanup(b.Close)

			createEngineCluster(t, b, "1.3.0.0")
			waitCluster(t, b, "c1", "failed")

			require.Eventually(t, func() bool {
				return len(tt.rt.createdSpecs()) == len(tt.rt.removedIDs())
			}, 10*time.Second, 5*time.Millisecond)
		})
	}
}

func TestEngineStopStartCluster(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, readyProbe)
	createEngineCluster(t, b, "1.3.0.0")
	before := waitCluster(t, b, "c1", "available")

	_, err := b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)

	stopped := waitCluster(t, b, "c1", "stopped")
	stops := rt.stoppedIDs()
	require.Len(t, stops, 1)
	assert.Equal(t, before.Port, stopped.Port)

	_, err = b.StartDBCluster(t.Context(), "c1")
	require.NoError(t, err)

	started := waitCluster(t, b, "c1", "available")
	assert.Equal(t, stops, rt.startedIDs())
	assert.Equal(t, before.Port, started.Port)
	assert.Equal(t, "127.0.0.1", started.Endpoint)
	assert.Empty(t, rt.removedIDs())
}

func TestEngineStopRequiresAvailable(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	gate := newGatedProbe()
	b := engineBackend(t, rt, gate.probe)
	createEngineCluster(t, b, "1.3.0.0")

	_, err := b.StopDBCluster(t.Context(), "c1")
	require.ErrorIs(t, err, neptune.ErrInvalidDBClusterStateFault)
	assert.Empty(t, rt.stoppedIDs())
}

func TestEngineRemovesContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		teardown func(t *testing.T, b *neptune.InMemoryBackend)
		name     string
	}{
		{
			name: "delete_cluster",
			teardown: func(t *testing.T, b *neptune.InMemoryBackend) {
				t.Helper()

				_, err := b.DeleteDBCluster(t.Context(), "c1", neptune.DBClusterDeleteOptions{SkipFinalSnapshot: true})
				require.NoError(t, err)
			},
		},
		{name: "reset", teardown: func(_ *testing.T, b *neptune.InMemoryBackend) { b.Reset() }},
		{name: "close", teardown: func(_ *testing.T, b *neptune.InMemoryBackend) { b.Close() }},
		{
			name: "shutdown",
			teardown: func(t *testing.T, b *neptune.InMemoryBackend) {
				t.Helper()
				neptune.NewHandler(b).Shutdown(t.Context())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeEngineRuntime{}
			b := engineBackend(t, rt, readyProbe)
			createEngineCluster(t, b, "1.3.0.0")
			waitCluster(t, b, "c1", "available")

			tt.teardown(t, b)

			require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
			assert.Equal(t, "ctr-"+rt.createdSpecs()[0].Name, rt.removedIDs()[0])
		})
	}
}

func TestEngineDeleteDuringStartupRemovesContainer(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	gate := newGatedProbe()
	b := engineBackend(t, rt, gate.probe)
	createEngineCluster(t, b, "1.3.0.0")

	require.Eventually(t, func() bool { return len(rt.createdSpecs()) == 1 }, 10*time.Second, 5*time.Millisecond)

	_, err := b.DeleteDBCluster(t.Context(), "c1", neptune.DBClusterDeleteOptions{SkipFinalSnapshot: true})
	require.NoError(t, err)

	require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
}

func TestEngineRestoreStartsContainer(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, readyProbe)
	createEngineCluster(t, b, "1.3.0.0")
	waitCluster(t, b, "c1", "available")

	_, err := b.CreateDBClusterSnapshot(t.Context(), "snap1", "c1")
	require.NoError(t, err)

	tests := []struct {
		restore func() (*neptune.DBCluster, error)
		name    string
	}{
		{name: "snapshot", restore: func() (*neptune.DBCluster, error) {
			return b.RestoreDBClusterFromSnapshot(t.Context(), "snap1", "r1", neptune.RestoreClusterOptions{})
		}},
		{name: "pitr", restore: func() (*neptune.DBCluster, error) {
			return b.RestoreDBClusterToPointInTime(t.Context(), "c1", "r2", neptune.RestoreToPointInTimeOptions{
				UseLatestRestorableTime: true,
			})
		}},
	}

	for _, tt := range tests {
		restored, rerr := tt.restore()
		require.NoError(t, rerr, tt.name)
		assert.Equal(t, "creating", restored.Status, tt.name)
	}

	r1, r2 := waitCluster(t, b, "r1", "available"), waitCluster(t, b, "r2", "available")
	c1 := describeCluster(t, b, "c1")

	assert.Len(t, rt.createdSpecs(), 3)
	assert.Equal(t, "127.0.0.1", r1.Endpoint)
	assert.NotEqual(t, c1.Port, r1.Port)
	assert.NotEqual(t, r1.Port, r2.Port)
}

func TestEngineRenameKeepsContainer(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, readyProbe)
	createEngineCluster(t, b, "1.3.0.0")
	before := waitCluster(t, b, "c1", "available")

	_, err := b.ModifyDBCluster(t.Context(), "c1", "", neptune.DBClusterModifyOptions{NewDBClusterIdentifier: "c2"})
	require.NoError(t, err)

	renamed := describeCluster(t, b, "c2")
	assert.Equal(t, "127.0.0.1", renamed.Endpoint)
	assert.Equal(t, before.Port, renamed.Port)

	_, err = b.DeleteDBCluster(t.Context(), "c2", neptune.DBClusterDeleteOptions{SkipFinalSnapshot: true})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
}

func TestEngineSnapshotRestoreRelaunches(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	src := engineBackend(t, rt, readyProbe)
	createEngineCluster(t, src, "1.3.0.0")
	waitCluster(t, src, "c1", "available")

	_, err := src.CreateDBInstance(t.Context(), "i1", "c1", "", neptune.DBInstanceCreateOptions{})
	require.NoError(t, err)

	snap := src.Snapshot(t.Context())
	require.NotNil(t, snap)

	rt2 := &fakeEngineRuntime{}
	gate := newGatedProbe()
	dst := engineBackend(t, rt2, gate.probe)
	require.NoError(t, dst.Restore(t.Context(), snap))

	assert.Equal(t, "creating", describeCluster(t, dst, "c1").Status)
	assert.Equal(t, "creating", describeInstance(t, dst).DBInstanceStatus)

	close(gate.gate)
	c := waitCluster(t, dst, "c1", "available")

	require.Len(t, rt2.createdSpecs(), 1)
	assert.Equal(t, "127.0.0.1", c.Endpoint)
	assert.Equal(t, publishedPort(t, rt2.createdSpecs()[0]), c.Port)
}
