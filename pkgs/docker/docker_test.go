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

func TestClient_Ping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pingErr error
		wantErr error
	}{
		{name: "success"},
		{name: "failure", pingErr: errDaemonNotRunning, wantErr: docker.ErrDockerUnavailable},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI()
			api.pingError = tt.pingErr
			c := docker.NewClientWithAPI(api, docker.Config{})

			err := c.Ping(t.Context())

			if tt.wantErr != nil {
				require.Error(t, err)
				assert.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestClient_PullImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		pullErr error
		wantErr bool
	}{
		{name: "success"},
		{name: "error", pullErr: errNetworkError, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI()
			api.pullError = tt.pullErr
			c := docker.NewClientWithAPI(api, docker.Config{})

			err := c.PullImage(t.Context(), "alpine:latest")

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestClient_HasImage(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ref     string
		images  []image.Summary
		listErr error
		wantOK  bool
		wantErr bool
	}{
		{
			name:   "present",
			ref:    "alpine:latest",
			images: []image.Summary{{RepoTags: []string{"alpine:latest"}}},
			wantOK: true,
		},
		{name: "absent", ref: "nonexistent:latest"},
		{name: "list_error", ref: "alpine:latest", listErr: errDockerDaemon, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI()
			api.images = tt.images
			api.listError = tt.listErr
			c := docker.NewClientWithAPI(api, docker.Config{})

			ok, err := c.HasImage(t.Context(), tt.ref)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantOK, ok)
		})
	}
}

func TestClient_CreateAndStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		spec      docker.ContainerSpec
		createErr error
		startErr  error
		wantErr   bool
	}{
		{
			name: "basic",
			spec: docker.ContainerSpec{
				Image: "alpine:latest",
				Env:   []string{"FOO=bar"},
				Cmd:   []string{"sh", "-c", "echo hello"},
			},
		},
		{
			name: "with_mounts",
			spec: docker.ContainerSpec{
				Image:  "alpine:latest",
				Mounts: []string{"/tmp:/tmp:ro"},
			},
		},
		{
			name:      "create_error",
			spec:      docker.ContainerSpec{Image: "bad:image"},
			createErr: errImageNotFound,
			wantErr:   true,
		},
		{
			name:    "start_error",
			spec:    docker.ContainerSpec{Image: "alpine:latest"},
			startErr: errFailedToStart,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI()
			api.createError = tt.createErr
			api.startError = tt.startErr
			c := docker.NewClientWithAPI(api, docker.Config{})

			id, err := c.CreateAndStart(t.Context(), tt.spec)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.NotEmpty(t, id)
		})
	}
}

func TestClient_StopAndRemove(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(t *testing.T, api *mockDockerAPI, c *docker.Client) string
		wantErr bool
	}{
		{
			name: "success",
			setup: func(t *testing.T, _ *mockDockerAPI, c *docker.Client) string {
				t.Helper()
				id, err := c.CreateAndStart(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})
				require.NoError(t, err)

				return id
			},
		},
		{
			name: "stop_error",
			setup: func(_ *testing.T, api *mockDockerAPI, _ *docker.Client) string {
				api.stopError = errFailedToStop

				return "some-id"
			},
			wantErr: true,
		},
		{
			name: "remove_error",
			setup: func(_ *testing.T, api *mockDockerAPI, _ *docker.Client) string {
				api.removeError = errFailedToRemove

				return "some-id"
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			api := newMockAPI()
			c := docker.NewClientWithAPI(api, docker.Config{})
			id := tt.setup(t, api, c)

			err := c.StopAndRemove(t.Context(), id)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestClient_AcquireWarm_NewContainer(t *testing.T) {
	t.Parallel()

	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 2})

	pc, err := c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})

	require.NoError(t, err)
	assert.True(t, pc.InUse)
	assert.Equal(t, "alpine:latest", pc.Image)
}

func TestClient_AcquireWarm_ReusesIdle(t *testing.T) {
	t.Parallel()

	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 2})

	pc1, err := c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)

	err = c.ReleaseContainer(pc1.ID)
	require.NoError(t, err)

	pc2, err := c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)

	assert.Equal(t, pc1.ID, pc2.ID, "should reuse the idle container")
}

func TestClient_AcquireWarm_Exhausted(t *testing.T) {
	t.Parallel()

	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 1})

	_, err := c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)

	_, err = c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})
	assert.ErrorIs(t, err, docker.ErrPoolExhausted)
}

func TestClient_AcquireWarm_CreateError(t *testing.T) {
	t.Parallel()

	api := newMockAPI()
	api.createError = errImageNotFound
	c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 2})

	_, err := c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "bad:image"})
	assert.Error(t, err)
}

func TestClient_ReleaseContainer_NotFound(t *testing.T) {
	t.Parallel()

	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{})

	err := c.ReleaseContainer("nonexistent-id")
	assert.ErrorIs(t, err, docker.ErrContainerNotFound)
}

func TestClient_ReapIdleContainers(t *testing.T) {
	t.Parallel()

	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{
		PoolSize:    2,
		IdleTimeout: 1 * time.Millisecond,
	})

	pc, err := c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)

	err = c.ReleaseContainer(pc.ID)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	c.ReapIdleContainers(t.Context())

	pc2, err := c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)
	assert.NotEqual(t, pc.ID, pc2.ID, "old container should have been reaped")
}

func TestClient_ReapIdleContainers_ReapError(t *testing.T) {
	t.Parallel()

	api := newMockAPI()
	api.stopError = errFailedToStop
	c := docker.NewClientWithAPI(api, docker.Config{
		PoolSize:    2,
		IdleTimeout: 1 * time.Millisecond,
	})

	pc, err := c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)

	err = c.ReleaseContainer(pc.ID)
	require.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	c.ReapIdleContainers(t.Context())
}

func TestClient_ReapIdleContainers_WithLogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		stopErr error
	}{
		{name: "success"},
		{name: "error", stopErr: errFailedToStop},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
			api := newMockAPI()
			api.stopError = tt.stopErr
			c := docker.NewClientWithAPI(api, docker.Config{
				PoolSize:    2,
				IdleTimeout: 1 * time.Millisecond,
				Logger:      log,
			})

			pc, err := c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})
			require.NoError(t, err)

			err = c.ReleaseContainer(pc.ID)
			require.NoError(t, err)

			time.Sleep(10 * time.Millisecond)

			c.ReapIdleContainers(t.Context())
		})
	}
}

func TestClient_StartReaper(t *testing.T) {
	t.Parallel()

	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{
		PoolSize:    2,
		IdleTimeout: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(t.Context())

	c.StartReaper(ctx, 1*time.Millisecond)

	pc, err := c.AcquireWarm(t.Context(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)

	err = c.ReleaseContainer(pc.ID)
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	cancel()
}

func TestClient_StartReaper_DefaultInterval(t *testing.T) {
	t.Parallel()

	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{
		PoolSize:    2,
		IdleTimeout: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(t.Context())
	c.StartReaper(ctx, 0)

	cancel()
}

func TestClient_Close(t *testing.T) {
	t.Parallel()

	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{})

	err := c.Close()
	assert.NoError(t, err)
}
