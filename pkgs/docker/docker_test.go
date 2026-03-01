package docker_test

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

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/docker"
)

// mockDockerAPI is a test double for docker.APIClient.
type mockDockerAPI struct {
	pullError   error
	createError error
	startError  error
	stopError   error
	removeError error
	listError   error
	pingError   error
	containers  map[string]string
	images      []image.Summary
	counter     int
	mu          sync.Mutex
}

// Sentinel errors used in tests.
var (
	errDaemonNotRunning = errors.New("daemon not running")
	errNetworkError     = errors.New("network error")
	errImageNotFound    = errors.New("image not found")
	errFailedToStart    = errors.New("failed to start")
	errFailedToStop     = errors.New("failed to stop")
	errFailedToRemove   = errors.New("failed to remove")
	errDockerDaemon     = errors.New("docker daemon error")
)

func newMockAPI() *mockDockerAPI {
	return &mockDockerAPI{
		containers: make(map[string]string),
	}
}

func (m *mockDockerAPI) ImagePull(_ context.Context, _ string, _ image.PullOptions) (io.ReadCloser, error) {
	if m.pullError != nil {
		return nil, m.pullError
	}

	return io.NopCloser(strings.NewReader("pull output")), nil
}

func (m *mockDockerAPI) ImageList(_ context.Context, opts image.ListOptions) ([]image.Summary, error) {
	if m.listError != nil {
		return nil, m.listError
	}

	refs := opts.Filters.Get("reference")
	ref := ""

	if len(refs) > 0 {
		ref = refs[0]
	}

	var out []image.Summary

	for _, img := range m.images {
		for _, tag := range img.RepoTags {
			if ref == "" || tag == ref {
				out = append(out, img)

				break
			}
		}
	}

	return out, nil
}

func (m *mockDockerAPI) ContainerCreate(
	_ context.Context,
	cfg *container.Config,
	_ *container.HostConfig,
	_ any,
	_ any,
	_ string,
) (container.CreateResponse, error) {
	if m.createError != nil {
		return container.CreateResponse{}, m.createError
	}

	m.mu.Lock()
	m.counter++
	id := fmt.Sprintf("container-%d", m.counter)
	m.containers[id] = cfg.Image
	m.mu.Unlock()

	return container.CreateResponse{ID: id}, nil
}

func (m *mockDockerAPI) ContainerStart(_ context.Context, _ string, _ container.StartOptions) error {
	return m.startError
}

func (m *mockDockerAPI) ContainerStop(_ context.Context, _ string, _ container.StopOptions) error {
	return m.stopError
}

func (m *mockDockerAPI) ContainerRemove(_ context.Context, containerID string, _ container.RemoveOptions) error {
	if m.removeError != nil {
		return m.removeError
	}

	m.mu.Lock()
	delete(m.containers, containerID)
	m.mu.Unlock()

	return nil
}

func (m *mockDockerAPI) Ping(_ context.Context) (any, error) {
	if m.pingError != nil {
		return nil, m.pingError
	}

	return struct{}{}, nil
}

func (m *mockDockerAPI) Close() error {
	return nil
}

func TestDocker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "Ping_Success", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{})

			err := c.Ping(context.Background())
			assert.NoError(t, err)
		}},
		{name: "Ping_Failure", run: func(t *testing.T) {
			api := newMockAPI()
			api.pingError = errDaemonNotRunning
			c := docker.NewClientWithAPI(api, docker.Config{})

			err := c.Ping(context.Background())
			assert.ErrorIs(t, err, docker.ErrDockerUnavailable)
		}},
		{name: "PullImage_Success", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{})

			err := c.PullImage(context.Background(), "alpine:latest")
			assert.NoError(t, err)
		}},
		{name: "PullImage_Error", run: func(t *testing.T) {
			api := newMockAPI()
			api.pullError = errNetworkError
			c := docker.NewClientWithAPI(api, docker.Config{})

			err := c.PullImage(context.Background(), "alpine:latest")
			assert.Error(t, err)
		}},
		{name: "HasImage_Present", run: func(t *testing.T) {
			api := newMockAPI()
			api.images = []image.Summary{
				{RepoTags: []string{"alpine:latest"}},
			}
			c := docker.NewClientWithAPI(api, docker.Config{})

			ok, err := c.HasImage(context.Background(), "alpine:latest")
			require.NoError(t, err)
			assert.True(t, ok)
		}},
		{name: "HasImage_Absent", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{})

			ok, err := c.HasImage(context.Background(), "nonexistent:latest")
			require.NoError(t, err)
			assert.False(t, ok)
		}},
		{name: "CreateAndStart", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{})

			id, err := c.CreateAndStart(context.Background(), docker.ContainerSpec{
				Image: "alpine:latest",
				Env:   []string{"FOO=bar"},
				Cmd:   []string{"sh", "-c", "echo hello"},
			})
			require.NoError(t, err)
			assert.NotEmpty(t, id)
		}},
		{name: "CreateAndStart_WithMounts", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{})

			id, err := c.CreateAndStart(context.Background(), docker.ContainerSpec{
				Image:  "alpine:latest",
				Mounts: []string{"/tmp:/tmp:ro"},
			})
			require.NoError(t, err)
			assert.NotEmpty(t, id)
		}},
		{name: "CreateAndStart_CreateError", run: func(t *testing.T) {
			api := newMockAPI()
			api.createError = errImageNotFound
			c := docker.NewClientWithAPI(api, docker.Config{})

			_, err := c.CreateAndStart(context.Background(), docker.ContainerSpec{Image: "bad:image"})
			assert.Error(t, err)
		}},
		{name: "CreateAndStart_StartError", run: func(t *testing.T) {
			api := newMockAPI()
			api.startError = errFailedToStart
			c := docker.NewClientWithAPI(api, docker.Config{})

			_, err := c.CreateAndStart(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			assert.Error(t, err)
		}},
		{name: "StopAndRemove", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{})

			id, err := c.CreateAndStart(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)

			err = c.StopAndRemove(context.Background(), id)
			assert.NoError(t, err)
		}},
		{name: "StopAndRemove_StopError", run: func(t *testing.T) {
			api := newMockAPI()
			api.stopError = errFailedToStop
			c := docker.NewClientWithAPI(api, docker.Config{})

			err := c.StopAndRemove(context.Background(), "some-id")
			assert.Error(t, err)
		}},
		{name: "StopAndRemove_RemoveError", run: func(t *testing.T) {
			api := newMockAPI()
			api.removeError = errFailedToRemove
			c := docker.NewClientWithAPI(api, docker.Config{})

			err := c.StopAndRemove(context.Background(), "some-id")
			assert.Error(t, err)
		}},
		{name: "AcquireWarm_NewContainer", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 2})

			pc, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)
			assert.True(t, pc.InUse)
			assert.Equal(t, "alpine:latest", pc.Image)
		}},
		{name: "AcquireWarm_ReusesIdle", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 2})

			pc1, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)

			err = c.ReleaseContainer(pc1.ID)
			require.NoError(t, err)

			pc2, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)

			assert.Equal(t, pc1.ID, pc2.ID, "should reuse the idle container")
		}},
		{name: "AcquireWarm_Exhausted", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 1})

			_, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)

			_, err = c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			assert.ErrorIs(t, err, docker.ErrPoolExhausted)
		}},
		{name: "AcquireWarm_CreateError", run: func(t *testing.T) {
			api := newMockAPI()
			api.createError = errImageNotFound
			c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 2})

			_, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "bad:image"})
			assert.Error(t, err)
		}},
		{name: "ReleaseContainer_NotFound", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{})

			err := c.ReleaseContainer("nonexistent-id")
			assert.ErrorIs(t, err, docker.ErrContainerNotFound)
		}},
		{name: "ReapIdleContainers", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{
				PoolSize:    2,
				IdleTimeout: 1 * time.Millisecond,
			})

			pc, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)

			err = c.ReleaseContainer(pc.ID)
			require.NoError(t, err)

			// Wait for idle timeout to expire.
			time.Sleep(10 * time.Millisecond)

			c.ReapIdleContainers(context.Background())

			// Container should have been reaped; acquiring again creates a new one.
			pc2, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)
			assert.NotEqual(t, pc.ID, pc2.ID, "old container should have been reaped")
		}},
		{name: "ReapIdleContainers_ReapError", run: func(t *testing.T) {
			api := newMockAPI()
			api.stopError = errFailedToStop
			c := docker.NewClientWithAPI(api, docker.Config{
				PoolSize:    2,
				IdleTimeout: 1 * time.Millisecond,
			})

			pc, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)

			err = c.ReleaseContainer(pc.ID)
			require.NoError(t, err)

			time.Sleep(10 * time.Millisecond)

			// Should not panic even if reap fails.
			c.ReapIdleContainers(context.Background())
		}},
		{name: "StartReaper", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{
				PoolSize:    2,
				IdleTimeout: 5 * time.Millisecond,
			})

			ctx, cancel := context.WithCancel(context.Background())

			c.StartReaper(ctx, 1*time.Millisecond)

			pc, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)

			err = c.ReleaseContainer(pc.ID)
			require.NoError(t, err)

			time.Sleep(50 * time.Millisecond)

			cancel() // stop the reaper
		}},
		{name: "Close", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{})

			err := c.Close()
			assert.NoError(t, err)
		}},
		{name: "HasImage_ListError", run: func(t *testing.T) {
			api := newMockAPI()
			api.listError = errDockerDaemon
			c := docker.NewClientWithAPI(api, docker.Config{})

			_, err := c.HasImage(context.Background(), "alpine:latest")
			assert.Error(t, err)
		}},
		{name: "ReapIdleContainers_WithLogger_Success", run: func(t *testing.T) {
			log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{
				PoolSize:    2,
				IdleTimeout: 1 * time.Millisecond,
				Logger:      log,
			})

			pc, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)

			err = c.ReleaseContainer(pc.ID)
			require.NoError(t, err)

			time.Sleep(10 * time.Millisecond)

			// Should log "reaped idle container" at debug level.
			c.ReapIdleContainers(context.Background())
		}},
		{name: "ReapIdleContainers_WithLogger_Error", run: func(t *testing.T) {
			log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
			api := newMockAPI()
			api.stopError = errFailedToStop
			c := docker.NewClientWithAPI(api, docker.Config{
				PoolSize:    2,
				IdleTimeout: 1 * time.Millisecond,
				Logger:      log,
			})

			pc, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)

			err = c.ReleaseContainer(pc.ID)
			require.NoError(t, err)

			time.Sleep(10 * time.Millisecond)

			// Should log "failed to reap idle container" at warn level.
			c.ReapIdleContainers(context.Background())
		}},
		{name: "StartReaper_DefaultInterval", run: func(t *testing.T) {
			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{
				PoolSize:    2,
				IdleTimeout: 5 * time.Millisecond,
			})

			ctx, cancel := context.WithCancel(context.Background())
			// Pass 0 interval to use default (half of IdleTimeout).
			c.StartReaper(ctx, 0)

			cancel()
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
