package redshift_test

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
	"github.com/blackbirdworks/gopherstack/services/redshift"
)

var errRuntimeDown = errors.New("runtime down")

type fakeEngineRuntime struct {
	stopGate  chan struct{}
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

func (f *fakeEngineRuntime) StopContainer(ctx context.Context, id string) error {
	if f.stopGate != nil {
		select {
		case <-f.stopGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

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

func (f *fakeEngineRuntime) lists() ([]container.Spec, []string, []string, []string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.specs...), append([]string(nil), f.removed...),
		append([]string(nil), f.stopped...), append([]string(nil), f.started...)
}

func (f *fakeEngineRuntime) createdSpecs() []container.Spec {
	s, _, _, _ := f.lists()

	return s
}

func (f *fakeEngineRuntime) removedIDs() []string {
	_, r, _, _ := f.lists()

	return r
}

type gatedProbe struct {
	gate   chan struct{}
	logins []redshift.EngineLogin
	mu     sync.Mutex
}

func newGatedProbe() *gatedProbe { return &gatedProbe{gate: make(chan struct{})} }

func (g *gatedProbe) probe(ctx context.Context, l redshift.EngineLogin) error {
	g.mu.Lock()
	g.logins = append(g.logins, l)
	g.mu.Unlock()

	select {
	case <-g.gate:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func readyProbe(context.Context, redshift.EngineLogin) error { return nil }

func engineBackend(
	t *testing.T, rt *fakeEngineRuntime, cfg redshift.EngineConfig,
) *redshift.InMemoryBackend {
	t.Helper()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	cfg.Runtime = rt
	cfg.StartTimeout = time.Minute

	if cfg.Probe == nil {
		cfg.Probe = readyProbe
	}

	b.EnableEngine(cfg)
	t.Cleanup(b.Close)

	return b
}

func createEngineCluster(t *testing.T, b *redshift.InMemoryBackend, password string) {
	t.Helper()

	_, err := b.CreateCluster("c1", "", "analytics", "admin1", nil, "",
		redshift.CreateClusterOptions{MasterUserPassword: password})
	require.NoError(t, err)
}

func clusterOf(b redshift.StorageBackend, id string) redshift.Cluster {
	cs, _, err := b.DescribeClusters(id, "", 0, nil, nil)
	if err != nil || len(cs) == 0 {
		return redshift.Cluster{}
	}

	return cs[0]
}

func waitStatus(t *testing.T, b redshift.StorageBackend, want string) redshift.Cluster {
	t.Helper()

	return waitStatusFor(t, b, want, 10*time.Second)
}

func waitStatusFor(t *testing.T, b redshift.StorageBackend, want string, wait time.Duration) redshift.Cluster {
	t.Helper()

	var c redshift.Cluster

	require.Eventually(t, func() bool {
		c = clusterOf(b, "c1")

		return c.Status == want
	}, wait, 5*time.Millisecond)

	return c
}

func publishedPort(t *testing.T, spec container.Spec) int {
	t.Helper()

	require.Len(t, spec.Ports, 1)
	parts := strings.Split(spec.Ports[0], ":")
	require.Len(t, parts, 3)
	assert.Equal(t, "5432", parts[2])

	p, err := strconv.Atoi(parts[1])
	require.NoError(t, err)

	return p
}

func TestEngineCreateClusterStartsPostgres(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		password string
		wantPass string
	}{
		{name: "explicit password", password: "Passw0rdXyz", wantPass: "Passw0rdXyz"},
		{name: "generated password"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeEngineRuntime{}
			gp := newGatedProbe()
			b := engineBackend(t, rt, redshift.EngineConfig{Probe: gp.probe})
			createEngineCluster(t, b, tt.password)

			assert.Equal(t, "creating", clusterOf(b, "c1").Status)
			require.Eventually(
				t,
				func() bool { return len(rt.createdSpecs()) == 1 },
				10*time.Second,
				5*time.Millisecond,
			)

			spec := rt.createdSpecs()[0]
			assert.Equal(t, "postgres:17", spec.Image)
			assert.True(t, strings.HasPrefix(spec.Name, "gopherstack-redshift-"))
			assert.Contains(t, spec.Env, "POSTGRES_USER=admin1")
			assert.Contains(t, spec.Env, "POSTGRES_DB=analytics")

			if tt.wantPass != "" {
				assert.Contains(t, spec.Env, "POSTGRES_PASSWORD="+tt.wantPass)
			} else {
				pw := ""
				for _, e := range spec.Env {
					pw = strings.TrimPrefix(e, "POSTGRES_PASSWORD=")
					if pw != e {
						break
					}
				}

				assert.NotEmpty(t, pw)
			}

			_, ready := b.EngineLogin("c1")
			assert.False(t, ready)

			close(gp.gate)

			c := waitStatus(t, b, "available")
			assert.Equal(t, "127.0.0.1", c.Endpoint)
			assert.Equal(t, publishedPort(t, spec), c.Port)

			login, ready := b.EngineLogin("c1")
			require.True(t, ready)
			assert.Equal(t, "admin1", login.User)
			assert.Equal(t, "analytics", login.DBName)
			assert.Equal(t, "127.0.0.1:"+strconv.Itoa(c.Port), login.Addr)
		})
	}
}

func TestEngineStartFailureMarksCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		rt   *fakeEngineRuntime
		cfg  redshift.EngineConfig
	}{
		{name: "runtime down", rt: &fakeEngineRuntime{createErr: errRuntimeDown}},
		{
			name: "probe never answers",
			rt:   &fakeEngineRuntime{},
			cfg: redshift.EngineConfig{
				Probe: func(context.Context, redshift.EngineLogin) error { return errRuntimeDown },
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			tt.cfg.Runtime = tt.rt
			b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
			tt.cfg.StartTimeout = 50 * time.Millisecond
			b.EnableEngine(tt.cfg)
			t.Cleanup(b.Close)

			createEngineCluster(t, b, "")
			waitStatus(t, b, "hardware-failure")

			_, ready := b.EngineLogin("c1")
			assert.False(t, ready)
		})
	}
}

func TestEnginePauseResumeRebootDelete(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{stopGate: make(chan struct{}, 2)}
	b := engineBackend(t, rt, redshift.EngineConfig{})
	createEngineCluster(t, b, "")
	created := waitStatus(t, b, "available")
	id := "ctr-" + rt.createdSpecs()[0].Name

	_, err := b.PauseCluster("c1")
	require.NoError(t, err)
	assert.Equal(t, "pausing", clusterOf(b, "c1").Status)
	rt.stopGate <- struct{}{}
	waitStatus(t, b, "paused")

	_, _, stopped, _ := rt.lists()
	assert.Equal(t, []string{id}, stopped)

	_, ready := b.EngineLogin("c1")
	assert.False(t, ready)

	_, err = b.ResumeCluster("c1")
	require.NoError(t, err)

	resumed := waitStatus(t, b, "available")
	assert.Equal(t, created.Port, resumed.Port)

	_, err = b.RebootCluster("c1")
	require.NoError(t, err)
	assert.Equal(t, "rebooting", clusterOf(b, "c1").Status)
	rt.stopGate <- struct{}{}
	waitStatus(t, b, "available")

	_, _, stopped, started := rt.lists()
	assert.Equal(t, []string{id, id}, stopped)
	assert.Equal(t, []string{id, id}, started)

	_, err = b.DeleteCluster("c1")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{id}, rt.removedIDs())
}

func TestEngineModifyCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setErr       error
		name         string
		wantPassword string
		opts         redshift.ModifyClusterOptions
		wantErr      bool
	}{
		{
			name:         "password applied",
			opts:         redshift.ModifyClusterOptions{MasterUserPassword: "N3wPassword"},
			wantPassword: "N3wPassword",
		},
		{
			name: "password failure", opts: redshift.ModifyClusterOptions{MasterUserPassword: "N3wPassword"},
			setErr: errRuntimeDown, wantErr: true,
		},
		{name: "port change keeps published port", opts: redshift.ModifyClusterOptions{Port: 6000}},
		{name: "rename keeps engine", opts: redshift.ModifyClusterOptions{NewClusterIdentifier: "renamed"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var (
				mu     sync.Mutex
				change redshift.PasswordChange
			)

			rt := &fakeEngineRuntime{}
			b := engineBackend(t, rt, redshift.EngineConfig{
				SetPassword: func(_ context.Context, c redshift.PasswordChange) error {
					mu.Lock()
					defer mu.Unlock()

					change = c

					return tt.setErr
				},
			})
			createEngineCluster(t, b, "Passw0rdXyz")
			before := waitStatus(t, b, "available")

			got, err := b.ModifyCluster("c1", tt.opts)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, before.Port, got.Port)
			assert.Equal(t, "127.0.0.1", got.Endpoint)

			mu.Lock()
			assert.Equal(t, tt.wantPassword, change.NewPassword)

			if tt.wantPassword != "" {
				assert.Equal(t, "Passw0rdXyz", change.Login.Password)
			}
			mu.Unlock()

			_, ready := b.EngineLogin(got.ClusterIdentifier)
			assert.True(t, ready)
		})
	}
}

func TestEngineResetAndCloseRemoveContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stop func(b *redshift.InMemoryBackend)
		name string
	}{
		{name: "reset", stop: func(b *redshift.InMemoryBackend) { b.Reset() }},
		{name: "close", stop: func(b *redshift.InMemoryBackend) { b.Close() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeEngineRuntime{}
			b := engineBackend(t, rt, redshift.EngineConfig{})
			createEngineCluster(t, b, "")
			waitStatus(t, b, "available")

			tt.stop(b)
			require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
		})
	}
}

func TestEngineRestoreRelaunchesClusters(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, redshift.EngineConfig{})
	createEngineCluster(t, b, "")
	waitStatus(t, b, "available")

	data := b.Snapshot(t.Context())
	require.NotEmpty(t, data)
	require.NoError(t, b.Restore(t.Context(), data))

	require.Eventually(t, func() bool { return len(rt.createdSpecs()) == 2 }, 10*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)

	c := waitStatus(t, b, "available")
	assert.Equal(t, publishedPort(t, rt.createdSpecs()[1]), c.Port)
}

func TestEngineSnapshotRestoreStartsContainer(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	b := engineBackend(t, rt, redshift.EngineConfig{})
	createEngineCluster(t, b, "")
	waitStatus(t, b, "available")

	_, err := b.CreateClusterSnapshot("snap1", "c1")
	require.NoError(t, err)

	_, err = b.RestoreFromClusterSnapshot("c2", "snap1", redshift.RestoreFromClusterSnapshotOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(rt.createdSpecs()) == 2 }, 10*time.Second, 5*time.Millisecond)

	require.Eventually(t, func() bool {
		return clusterOf(b, "c2").Status == "available"
	}, 10*time.Second, 5*time.Millisecond)
}

func TestEngineSiblingRegionsGetEngine(t *testing.T) {
	t.Parallel()

	rt := &fakeEngineRuntime{}
	home := engineBackend(t, rt, redshift.EngineConfig{})
	h := redshift.NewHandler(home)
	h.EnableRegions()

	sibling, ok := h.BackendFor("eu-west-1").(*redshift.InMemoryBackend)
	require.True(t, ok)

	_, err := sibling.CreateCluster("c1", "", "", "", nil, "", redshift.CreateClusterOptions{})
	require.NoError(t, err)
	waitStatus(t, sibling, "available")
	require.Len(t, rt.createdSpecs(), 1)

	h.Shutdown(t.Context())
	require.Eventually(t, func() bool { return len(rt.removedIDs()) == 1 }, 10*time.Second, 5*time.Millisecond)
}

func TestEngineDisabledKeepsMetadataOnly(t *testing.T) {
	t.Parallel()

	b := redshift.NewInMemoryBackend("000000000000", "us-east-1")
	t.Cleanup(b.Close)

	createEngineCluster(t, b, "")

	c := clusterOf(b, "c1")
	assert.Equal(t, "available", c.Status)
	assert.Contains(t, c.Endpoint, "redshift.amazonaws.com")
	assert.Equal(t, 5439, c.Port)

	_, ready := b.EngineLogin("c1")
	assert.False(t, ready)
}
