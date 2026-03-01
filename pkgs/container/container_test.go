package container_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// ---- sentinel errors used in tests ----

var (
	errDaemonDown      = errors.New("daemon down")
	errRegistryUnreach = errors.New("registry unreachable")
	errDaemonError     = errors.New("daemon error")
	errCreateFailed    = errors.New("create failed")
	errStartFailed     = errors.New("start failed")
	errStopFailed      = errors.New("stop failed")
	errRemoveFailed    = errors.New("remove failed")
)

// ---- mock APIClient for testing ----

type mockAPI struct {
	pullErr     error
	createErr   error
	startErr    error
	stopErr     error
	removeErr   error
	listErr     error
	pingErr     error
	images      []image.Summary
	counter     int
	closeCalled bool
	mu          sync.Mutex
}

func (m *mockAPI) ImagePull(_ context.Context, _ string, _ image.PullOptions) (io.ReadCloser, error) {
	if m.pullErr != nil {
		return nil, m.pullErr
	}

	return io.NopCloser(strings.NewReader("")), nil
}

func (m *mockAPI) ImageList(_ context.Context, _ image.ListOptions) ([]image.Summary, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}

	return m.images, nil
}

func (m *mockAPI) ContainerCreate(
	_ context.Context,
	_ *dockercontainer.Config,
	_ *dockercontainer.HostConfig,
	_ any,
	_ any,
	_ string,
) (dockercontainer.CreateResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.createErr != nil {
		return dockercontainer.CreateResponse{}, m.createErr
	}

	m.counter++

	return dockercontainer.CreateResponse{ID: fmt.Sprintf("ctr-%d", m.counter)}, nil
}

func (m *mockAPI) ContainerStart(_ context.Context, _ string, _ dockercontainer.StartOptions) error {
	return m.startErr
}

func (m *mockAPI) ContainerStop(_ context.Context, _ string, _ dockercontainer.StopOptions) error {
	return m.stopErr
}

func (m *mockAPI) ContainerRemove(_ context.Context, _ string, _ dockercontainer.RemoveOptions) error {
	return m.removeErr
}

func (m *mockAPI) Ping(_ context.Context) (any, error) {
	return struct{}{}, m.pingErr
}

func (m *mockAPI) Close() error {
	m.closeCalled = true

	return nil
}

// helper to build a DockerRuntime with a mock API.
func newRuntime(api container.APIClient) *container.DockerRuntime {
	return container.NewDockerRuntimeWithAPI(api, container.Config{
		PoolSize:    2,
		IdleTimeout: time.Minute,
	})
}

func TestNewRuntime_FromEnvVar(t *testing.T) {
	// Cannot use t.Parallel() with t.Setenv.
	t.Setenv("CONTAINER_RUNTIME", "podman")
	_, err := container.NewRuntime(container.Config{})
	if err != nil {
		assert.ErrorIs(t, err, container.ErrUnavailable)
	}
}

func TestDockerRuntime_Ping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pingErr error
		wantErr error
		name    string
	}{
		{name: "success"},
		{name: "daemon_down", pingErr: errDaemonDown, wantErr: container.ErrUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newRuntime(&mockAPI{pingErr: tt.pingErr})
			err := rt.Ping(t.Context())

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDockerRuntime_PullImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pullErr error
		name    string
		wantErr bool
	}{
		{name: "success"},
		{name: "registry_unreachable", pullErr: errRegistryUnreach, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newRuntime(&mockAPI{pullErr: tt.pullErr})
			err := rt.PullImage(t.Context(), "alpine:latest")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDockerRuntime_HasImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		listErr error
		name    string
		ref     string
		images  []image.Summary
		wantOK  bool
		wantErr bool
	}{
		{name: "present", ref: "alpine:latest", images: []image.Summary{{ID: "sha256:abc"}}, wantOK: true},
		{name: "absent", ref: "nonexistent:image"},
		{name: "error", ref: "alpine:latest", listErr: errDaemonError, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newRuntime(&mockAPI{images: tt.images, listErr: tt.listErr})
			ok, err := rt.HasImage(t.Context(), tt.ref)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestDockerRuntime_CreateAndStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		createErr error
		startErr  error
		wantID    string
		wantErr   bool
	}{
		{name: "success", wantID: "ctr-1"},
		{name: "create_error", createErr: errCreateFailed, wantErr: true},
		{name: "start_error", startErr: errStartFailed, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newRuntime(&mockAPI{createErr: tt.createErr, startErr: tt.startErr})
			id, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "alpine"})

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantID, id)
		})
	}
}

func TestDockerRuntime_StopAndRemove(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stopErr      error
		removeErr    error
		name         string
		wantContains string
		wantErr      bool
	}{
		{name: "success"},
		{name: "stop_error", stopErr: errStopFailed, wantErr: true},
		{name: "remove_error", removeErr: errRemoveFailed, wantErr: true, wantContains: "container remove"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newRuntime(&mockAPI{stopErr: tt.stopErr, removeErr: tt.removeErr})
			err := rt.StopAndRemove(t.Context(), "ctr-1")

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantContains != "" {
					assert.Contains(t, err.Error(), tt.wantContains)
				}

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestDockerRuntime_AcquireWarm_CreatesNew(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{})
	pc, err := rt.AcquireWarm(t.Context(), container.Spec{Image: "myimage"})

	require.NoError(t, err)
	assert.True(t, pc.InUse)
	assert.Equal(t, "myimage", pc.Image)
}

func TestDockerRuntime_AcquireWarm_ReusesIdle(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{})
	spec := container.Spec{Image: "reuse-image"}

	pc, err := rt.AcquireWarm(t.Context(), spec)
	require.NoError(t, err)
	require.NoError(t, rt.ReleaseContainer(pc.ID))

	pc2, err := rt.AcquireWarm(t.Context(), spec)
	require.NoError(t, err)
	assert.Equal(t, pc.ID, pc2.ID, "should reuse the idle container")
}

func TestDockerRuntime_AcquireWarm_PoolExhausted(t *testing.T) {
	t.Parallel()

	rt := container.NewDockerRuntimeWithAPI(&mockAPI{}, container.Config{PoolSize: 1})
	spec := container.Spec{Image: "limited-image"}

	_, err := rt.AcquireWarm(t.Context(), spec)
	require.NoError(t, err)

	_, err = rt.AcquireWarm(t.Context(), spec)
	require.ErrorIs(t, err, container.ErrPoolExhausted)
}

func TestDockerRuntime_AcquireWarm_CreateErr(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{createErr: errCreateFailed})
	_, err := rt.AcquireWarm(t.Context(), container.Spec{Image: "err-image"})

	require.Error(t, err)
}

func TestDockerRuntime_ReleaseContainer_NotFound(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{})
	err := rt.ReleaseContainer("nonexistent-id")

	require.ErrorIs(t, err, container.ErrContainerNotFound)
}

func TestDockerRuntime_ReapIdleContainers(t *testing.T) {
	t.Parallel()

	rt := container.NewDockerRuntimeWithAPI(&mockAPI{}, container.Config{
		PoolSize:    2,
		IdleTimeout: time.Nanosecond,
	})

	spec := container.Spec{Image: "reap-image"}
	pc, err := rt.AcquireWarm(t.Context(), spec)
	require.NoError(t, err)
	require.NoError(t, rt.ReleaseContainer(pc.ID))

	time.Sleep(10 * time.Millisecond)
	rt.ReapIdleContainers(t.Context())

	pc2, err := rt.AcquireWarm(t.Context(), spec)
	require.NoError(t, err)
	assert.NotEqual(t, pc.ID, pc2.ID, "reaped container should not be reused")
}

func TestDockerRuntime_ReapIdleContainers_WithLogger(t *testing.T) {
	t.Parallel()

	rt := container.NewDockerRuntimeWithAPI(&mockAPI{stopErr: errStopFailed}, container.Config{
		PoolSize:    2,
		IdleTimeout: time.Nanosecond,
		Logger:      slog.Default(),
	})

	spec := container.Spec{Image: "warn-image"}
	pc, err := rt.AcquireWarm(t.Context(), spec)
	require.NoError(t, err)
	require.NoError(t, rt.ReleaseContainer(pc.ID))

	time.Sleep(10 * time.Millisecond)
	rt.ReapIdleContainers(t.Context())
}

func TestDockerRuntime_StartReaper_StopsOnCancel(t *testing.T) {
	t.Parallel()

	rt := container.NewDockerRuntimeWithAPI(&mockAPI{}, container.Config{
		PoolSize:    1,
		IdleTimeout: time.Hour,
	})
	ctx, cancel := context.WithCancel(t.Context())
	rt.StartReaper(ctx, 10*time.Millisecond)
	cancel()
}

func TestDockerRuntime_StartReaper_ReapsAfterInterval(t *testing.T) {
	t.Parallel()

	rt := container.NewDockerRuntimeWithAPI(&mockAPI{}, container.Config{
		PoolSize:    2,
		IdleTimeout: time.Nanosecond,
	})

	spec := container.Spec{Image: "ticker-image"}
	pc, err := rt.AcquireWarm(t.Context(), spec)
	require.NoError(t, err)
	require.NoError(t, rt.ReleaseContainer(pc.ID))

	ctx, cancel := context.WithCancel(t.Context())
	rt.StartReaper(ctx, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	cancel()
}

func TestDockerRuntime_Close(t *testing.T) {
	t.Parallel()

	api := &mockAPI{}
	rt := newRuntime(api)

	require.NoError(t, rt.Close())
	assert.True(t, api.closeCalled)
}

func TestRuntimeName_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, container.RuntimeDocker, container.RuntimeName("docker"))
	assert.Equal(t, container.RuntimePodman, container.RuntimeName("podman"))
	assert.Equal(t, container.RuntimeAuto, container.RuntimeName("auto"))
}

func TestNewRuntime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		config  container.Config
	}{
		{name: "default_is_docker", config: container.Config{}},
		{
			name:    "unknown_runtime",
			config:  container.Config{Runtime: "invalid-runtime"},
			wantErr: container.ErrUnknownRuntime,
		},
		{name: "docker", config: container.Config{Runtime: container.RuntimeDocker}},
		{name: "podman", config: container.Config{Runtime: container.RuntimePodman}},
		{name: "auto", config: container.Config{Runtime: container.RuntimeAuto}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt, err := container.NewRuntime(tt.config)

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			if err != nil {
				assert.ErrorIs(t, err, container.ErrUnavailable)

				return
			}

			require.NoError(t, rt.Close())
		})
	}
}
