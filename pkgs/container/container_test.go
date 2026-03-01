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

func TestContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "DockerRuntime_Ping_OK", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{})
			require.NoError(t, rt.Ping(t.Context()))
		}},
		{name: "DockerRuntime_Ping_Err", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{pingErr: errDaemonDown})
			err := rt.Ping(t.Context())
			require.Error(t, err)
			assert.ErrorIs(t, err, container.ErrUnavailable)
		}},
		{name: "DockerRuntime_PullImage_OK", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{})
			require.NoError(t, rt.PullImage(t.Context(), "alpine:latest"))
		}},
		{name: "DockerRuntime_PullImage_Err", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{pullErr: errRegistryUnreach})
			err := rt.PullImage(t.Context(), "alpine:latest")
			require.Error(t, err)
		}},
		{name: "DockerRuntime_HasImage_True", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{images: []image.Summary{{ID: "sha256:abc"}}})
			ok, err := rt.HasImage(t.Context(), "alpine:latest")
			require.NoError(t, err)
			assert.True(t, ok)
		}},
		{name: "DockerRuntime_HasImage_False", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{})
			ok, err := rt.HasImage(t.Context(), "nonexistent:image")
			require.NoError(t, err)
			assert.False(t, ok)
		}},
		{name: "DockerRuntime_HasImage_Err", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{listErr: errDaemonError})
			_, err := rt.HasImage(t.Context(), "alpine:latest")
			require.Error(t, err)
		}},
		{name: "DockerRuntime_CreateAndStart_OK", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{})
			id, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "alpine"})
			require.NoError(t, err)
			assert.Equal(t, "ctr-1", id)
		}},
		{name: "DockerRuntime_CreateAndStart_CreateErr", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{createErr: errCreateFailed})
			_, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "alpine"})
			require.Error(t, err)
		}},
		{name: "DockerRuntime_CreateAndStart_StartErr", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{startErr: errStartFailed})
			_, err := rt.CreateAndStart(t.Context(), container.Spec{Image: "alpine"})
			require.Error(t, err)
		}},
		{name: "DockerRuntime_StopAndRemove_OK", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{})
			require.NoError(t, rt.StopAndRemove(t.Context(), "ctr-1"))
		}},
		{name: "DockerRuntime_StopAndRemove_StopErr", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{stopErr: errStopFailed})
			err := rt.StopAndRemove(t.Context(), "ctr-1")
			require.Error(t, err)
		}},
		{name: "DockerRuntime_StopAndRemove_RemoveErr", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{removeErr: errRemoveFailed})
			err := rt.StopAndRemove(t.Context(), "ctr-1")
			require.Error(t, err)
			assert.Contains(t, err.Error(), "container remove")
		}},
		{name: "DockerRuntime_AcquireWarm_CreatesNew", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{})
			pc, err := rt.AcquireWarm(t.Context(), container.Spec{Image: "myimage"})
			require.NoError(t, err)
			assert.True(t, pc.InUse)
			assert.Equal(t, "myimage", pc.Image)
		}},
		{name: "DockerRuntime_AcquireWarm_ReusesIdle", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{})
			spec := container.Spec{Image: "reuse-image"}

			pc, err := rt.AcquireWarm(t.Context(), spec)
			require.NoError(t, err)
			require.NoError(t, rt.ReleaseContainer(pc.ID))

			pc2, err := rt.AcquireWarm(t.Context(), spec)
			require.NoError(t, err)
			assert.Equal(t, pc.ID, pc2.ID, "should reuse the idle container")
		}},
		{name: "DockerRuntime_AcquireWarm_PoolExhausted", run: func(t *testing.T) {
			rt := container.NewDockerRuntimeWithAPI(&mockAPI{}, container.Config{PoolSize: 1})
			spec := container.Spec{Image: "limited-image"}

			_, err := rt.AcquireWarm(t.Context(), spec)
			require.NoError(t, err)

			_, err = rt.AcquireWarm(t.Context(), spec)
			require.ErrorIs(t, err, container.ErrPoolExhausted)
		}},
		{name: "DockerRuntime_AcquireWarm_CreateErr", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{createErr: errCreateFailed})
			_, err := rt.AcquireWarm(t.Context(), container.Spec{Image: "err-image"})
			require.Error(t, err)
		}},
		{name: "DockerRuntime_ReleaseContainer_NotFound", run: func(t *testing.T) {
			rt := newRuntime(&mockAPI{})
			err := rt.ReleaseContainer("nonexistent-id")
			require.ErrorIs(t, err, container.ErrContainerNotFound)
		}},
		{name: "DockerRuntime_ReapIdleContainers", run: func(t *testing.T) {
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
		}},
		{name: "DockerRuntime_ReapIdleContainers_WithLogger", run: func(t *testing.T) {
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
		}},
		{name: "DockerRuntime_StartReaper_StopsOnCancel", run: func(t *testing.T) {
			rt := container.NewDockerRuntimeWithAPI(&mockAPI{}, container.Config{
				PoolSize:    1,
				IdleTimeout: time.Hour,
			})
			ctx, cancel := context.WithCancel(t.Context())
			rt.StartReaper(ctx, 10*time.Millisecond)
			cancel()
		}},
		{name: "DockerRuntime_StartReaper_ReapsAfterInterval", run: func(t *testing.T) {
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
		}},
		{name: "DockerRuntime_Close", run: func(t *testing.T) {
			api := &mockAPI{}
			rt := newRuntime(api)
			require.NoError(t, rt.Close())
			assert.True(t, api.closeCalled)
		}},
		{name: "RuntimeName_Constants", run: func(t *testing.T) {
			assert.Equal(t, container.RuntimeDocker, container.RuntimeName("docker"))
			assert.Equal(t, container.RuntimePodman, container.RuntimeName("podman"))
			assert.Equal(t, container.RuntimeAuto, container.RuntimeName("auto"))
		}},
		{name: "NewRuntime_DefaultIsDocker", run: func(t *testing.T) {
			rt, err := container.NewRuntime(container.Config{})
			if err != nil {
				assert.ErrorIs(t, err, container.ErrUnavailable)
			} else {
				require.NoError(t, rt.Close())
			}
		}},
		{name: "NewRuntime_UnknownRuntimeName", run: func(t *testing.T) {
			_, err := container.NewRuntime(container.Config{Runtime: "invalid-runtime"})
			require.Error(t, err)
			assert.ErrorIs(t, err, container.ErrUnknownRuntime)
		}},
		{name: "NewRuntime_DockerUnavailable", run: func(t *testing.T) {
			rt, err := container.NewRuntime(container.Config{Runtime: container.RuntimeDocker})
			if err != nil {
				assert.ErrorIs(t, err, container.ErrUnavailable)
			} else {
				require.NoError(t, rt.Close())
			}
		}},
		{name: "NewRuntime_PodmanUnavailable", run: func(t *testing.T) {
			_, err := container.NewRuntime(container.Config{Runtime: container.RuntimePodman})
			if err != nil {
				assert.ErrorIs(t, err, container.ErrUnavailable)
			}
		}},
		{name: "NewRuntime_Auto", run: func(t *testing.T) {
			rt, err := container.NewRuntime(container.Config{Runtime: container.RuntimeAuto})
			if err != nil {
				assert.ErrorIs(t, err, container.ErrUnavailable)
			} else {
				require.NoError(t, rt.Close())
			}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
