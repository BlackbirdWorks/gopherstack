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

// ---- Ping ----

func TestDockerRuntime_Ping_OK(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{})
	require.NoError(t, rt.Ping(t.Context()))
}

func TestDockerRuntime_Ping_Err(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{pingErr: errDaemonDown})
	err := rt.Ping(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, container.ErrUnavailable)
}

// ---- PullImage ----

func TestDockerRuntime_PullImage_OK(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{})
	require.NoError(t, rt.PullImage(t.Context(), "alpine:latest"))
}

func TestDockerRuntime_PullImage_Err(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{pullErr: errRegistryUnreach})
	err := rt.PullImage(t.Context(), "alpine:latest")
	require.Error(t, err)
}

// ---- HasImage ----

func TestDockerRuntime_HasImage_True(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{images: []image.Summary{{ID: "sha256:abc"}}})
	ok, err := rt.HasImage(t.Context(), "alpine:latest")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestDockerRuntime_HasImage_False(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{})
	ok, err := rt.HasImage(t.Context(), "nonexistent:image")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestDockerRuntime_HasImage_Err(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{listErr: errDaemonError})
	_, err := rt.HasImage(t.Context(), "alpine:latest")
	require.Error(t, err)
}

// ---- CreateAndStart ----

func TestDockerRuntime_CreateAndStart_OK(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{})
	id, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "alpine"})
	require.NoError(t, err)
	assert.Equal(t, "ctr-1", id)
}

func TestDockerRuntime_CreateAndStart_CreateErr(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{createErr: errCreateFailed})
	_, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "alpine"})
	require.Error(t, err)
}

func TestDockerRuntime_CreateAndStart_StartErr(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{startErr: errStartFailed})
	_, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "alpine"})
	require.Error(t, err)
}

// ---- StopAndRemove ----

func TestDockerRuntime_StopAndRemove_OK(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{})
	require.NoError(t, rt.StopAndRemove(t.Context(), "ctr-1"))
}

func TestDockerRuntime_StopAndRemove_StopErr(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{stopErr: errStopFailed})
	err := rt.StopAndRemove(t.Context(), "ctr-1")
	require.Error(t, err)
}

// ---- AcquireWarm / ReleaseContainer ----

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

func TestDockerRuntime_ReleaseContainer_NotFound(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{})
	err := rt.ReleaseContainer("nonexistent-id")
	require.ErrorIs(t, err, container.ErrContainerNotFound)
}

// ---- ReapIdleContainers ----

func TestDockerRuntime_ReapIdleContainers(t *testing.T) {
	t.Parallel()

	rt := container.NewDockerRuntimeWithAPI(&mockAPI{}, container.Config{
		PoolSize:    2,
		IdleTimeout: time.Nanosecond, // immediately expired
	})

	spec := container.Spec{Image: "reap-image"}
	pc, err := rt.AcquireWarm(t.Context(), spec)
	require.NoError(t, err)
	require.NoError(t, rt.ReleaseContainer(pc.ID))

	// Wait long enough for the idle timeout to pass.
	time.Sleep(10 * time.Millisecond)
	rt.ReapIdleContainers(t.Context())

	// After reaping, the pool is empty; AcquireWarm creates a new one.
	pc2, err := rt.AcquireWarm(t.Context(), spec)
	require.NoError(t, err)
	assert.NotEqual(t, pc.ID, pc2.ID, "reaped container should not be reused")
}

// ---- StartReaper ----

func TestDockerRuntime_StartReaper_StopsOnCancel(t *testing.T) {
	t.Parallel()

	rt := container.NewDockerRuntimeWithAPI(&mockAPI{}, container.Config{
		PoolSize:    1,
		IdleTimeout: time.Hour,
	})
	ctx, cancel := context.WithCancel(t.Context())
	rt.StartReaper(ctx, 10*time.Millisecond)
	cancel() // should not block or panic
}

// ---- Close ----

func TestDockerRuntime_Close(t *testing.T) {
	t.Parallel()

	api := &mockAPI{}
	rt := newRuntime(api)
	require.NoError(t, rt.Close())
	assert.True(t, api.closeCalled)
}

func TestDockerRuntime_ReapIdleContainers_WithLogger(t *testing.T) {
	t.Parallel()

	// Use a stop error so the reaper logs a warning.
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
	// Should log a warning but not panic.
	rt.ReapIdleContainers(t.Context())
}

func TestDockerRuntime_StopAndRemove_RemoveErr(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{removeErr: errRemoveFailed})
	err := rt.StopAndRemove(t.Context(), "ctr-1")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "container remove")
}

func TestDockerRuntime_AcquireWarm_CreateErr(t *testing.T) {
	t.Parallel()

	rt := newRuntime(&mockAPI{createErr: errCreateFailed})
	_, err := rt.AcquireWarm(t.Context(), container.Spec{Image: "err-image"})
	require.Error(t, err)
}

func TestNewRuntime_FromEnvVar(t *testing.T) {
	// Cannot use t.Parallel() with t.Setenv.
	t.Setenv("CONTAINER_RUNTIME", "podman")
	// Podman socket not available in CI → expect ErrUnavailable.
	_, err := container.NewRuntime(container.Config{})
	if err != nil {
		assert.ErrorIs(t, err, container.ErrUnavailable)
	}
}

func TestNewRuntime_DefaultIsDocker(t *testing.T) {
	t.Parallel()

	// With no env var and no Runtime field, it should try Docker.
	// Either succeeds or returns ErrUnavailable — never panics.
	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		assert.ErrorIs(t, err, container.ErrUnavailable)
	} else {
		require.NoError(t, rt.Close())
	}
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
	// Use a very short interval so the ticker fires quickly.
	rt.StartReaper(ctx, 5*time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	cancel()
}

func TestRuntimeName_Constants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, container.RuntimeDocker, container.RuntimeName("docker"))
	assert.Equal(t, container.RuntimePodman, container.RuntimeName("podman"))
	assert.Equal(t, container.RuntimeAuto, container.RuntimeName("auto"))
}

// ---- NewRuntime factory / auto-detection ----

func TestNewRuntime_UnknownRuntimeName(t *testing.T) {
	t.Parallel()

	_, err := container.NewRuntime(container.Config{Runtime: "invalid-runtime"})
	require.Error(t, err)
	assert.ErrorIs(t, err, container.ErrUnknownRuntime)
}

func TestNewRuntime_DockerUnavailable(t *testing.T) {
	t.Parallel()

	// Docker is unlikely to be available in the test sandbox, but even if it is we
	// expect either success or ErrUnavailable — never a panic.
	rt, err := container.NewRuntime(container.Config{Runtime: container.RuntimeDocker})
	if err != nil {
		assert.ErrorIs(t, err, container.ErrUnavailable)
	} else {
		require.NoError(t, rt.Close())
	}
}

func TestNewRuntime_PodmanUnavailable(t *testing.T) {
	t.Parallel()

	// Podman socket is almost certainly not present in CI.
	_, err := container.NewRuntime(container.Config{Runtime: container.RuntimePodman})
	if err != nil {
		assert.ErrorIs(t, err, container.ErrUnavailable)
	}
}

func TestNewRuntime_Auto(t *testing.T) {
	t.Parallel()

	// Auto should return either a runtime or ErrUnavailable — never a panic.
	rt, err := container.NewRuntime(container.Config{Runtime: container.RuntimeAuto})
	if err != nil {
		assert.ErrorIs(t, err, container.ErrUnavailable)
	} else {
		require.NoError(t, rt.Close())
	}
}
