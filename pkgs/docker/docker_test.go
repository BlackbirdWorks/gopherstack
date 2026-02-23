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

// mockDockerAPI is a test double for DockerAPIClient.
type mockDockerAPI struct {
	mu          sync.Mutex
	containers  map[string]string // id → image
	images      []image.Summary
	pullError   error
	createError error
	startError  error
	stopError   error
	removeError error
	listError   error
	pingError   error
	counter     int
}

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

func (m *mockDockerAPI) ContainerCreate(_ context.Context, cfg *container.Config, _ *container.HostConfig, _ interface{}, _ interface{}, _ string) (container.CreateResponse, error) {
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

func (m *mockDockerAPI) Ping(_ context.Context) (interface{}, error) {
	if m.pingError != nil {
		return nil, m.pingError
	}

	return struct{}{}, nil
}

func (m *mockDockerAPI) Close() error {
	return nil
}

func TestPing_Success(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{})

	err := c.Ping(context.Background())
	assert.NoError(t, err)
}

func TestPing_Failure(t *testing.T) {
	api := newMockAPI()
	api.pingError = errors.New("daemon not running")
	c := docker.NewClientWithAPI(api, docker.Config{})

	err := c.Ping(context.Background())
	assert.ErrorIs(t, err, docker.ErrDockerUnavailable)
}

func TestPullImage_Success(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{})

	err := c.PullImage(context.Background(), "alpine:latest")
	assert.NoError(t, err)
}

func TestPullImage_Error(t *testing.T) {
	api := newMockAPI()
	api.pullError = errors.New("network error")
	c := docker.NewClientWithAPI(api, docker.Config{})

	err := c.PullImage(context.Background(), "alpine:latest")
	assert.Error(t, err)
}

func TestHasImage_Present(t *testing.T) {
	api := newMockAPI()
	api.images = []image.Summary{
		{RepoTags: []string{"alpine:latest"}},
	}
	c := docker.NewClientWithAPI(api, docker.Config{})

	ok, err := c.HasImage(context.Background(), "alpine:latest")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestHasImage_Absent(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{})

	ok, err := c.HasImage(context.Background(), "nonexistent:latest")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestCreateAndStart(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{})

	id, err := c.CreateAndStart(context.Background(), docker.ContainerSpec{
		Image: "alpine:latest",
		Env:   []string{"FOO=bar"},
		Cmd:   []string{"sh", "-c", "echo hello"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, id)
}

func TestCreateAndStart_WithMounts(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{})

	id, err := c.CreateAndStart(context.Background(), docker.ContainerSpec{
		Image:  "alpine:latest",
		Mounts: []string{"/tmp:/tmp:ro"},
	})
	require.NoError(t, err)
	assert.NotEmpty(t, id)
}

func TestCreateAndStart_CreateError(t *testing.T) {
	api := newMockAPI()
	api.createError = errors.New("image not found")
	c := docker.NewClientWithAPI(api, docker.Config{})

	_, err := c.CreateAndStart(context.Background(), docker.ContainerSpec{Image: "bad:image"})
	assert.Error(t, err)
}

func TestCreateAndStart_StartError(t *testing.T) {
	api := newMockAPI()
	api.startError = errors.New("failed to start")
	c := docker.NewClientWithAPI(api, docker.Config{})

	_, err := c.CreateAndStart(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
	assert.Error(t, err)
}

func TestStopAndRemove(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{})

	id, err := c.CreateAndStart(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)

	err = c.StopAndRemove(context.Background(), id)
	assert.NoError(t, err)
}

func TestStopAndRemove_StopError(t *testing.T) {
	api := newMockAPI()
	api.stopError = errors.New("failed to stop")
	c := docker.NewClientWithAPI(api, docker.Config{})

	err := c.StopAndRemove(context.Background(), "some-id")
	assert.Error(t, err)
}

func TestStopAndRemove_RemoveError(t *testing.T) {
	api := newMockAPI()
	api.removeError = errors.New("failed to remove")
	c := docker.NewClientWithAPI(api, docker.Config{})

	err := c.StopAndRemove(context.Background(), "some-id")
	assert.Error(t, err)
}

func TestAcquireWarm_NewContainer(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 2})

	pc, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)
	assert.True(t, pc.InUse)
	assert.Equal(t, "alpine:latest", pc.Image)
}

func TestAcquireWarm_ReusesIdle(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 2})

	pc1, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)

	err = c.ReleaseContainer(pc1.ID)
	require.NoError(t, err)

	pc2, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)

	assert.Equal(t, pc1.ID, pc2.ID, "should reuse the idle container")
}

func TestAcquireWarm_Exhausted(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 1})

	_, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
	require.NoError(t, err)

	_, err = c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "alpine:latest"})
	assert.ErrorIs(t, err, docker.ErrPoolExhausted)
}

func TestAcquireWarm_CreateError(t *testing.T) {
	api := newMockAPI()
	api.createError = errors.New("image not found")
	c := docker.NewClientWithAPI(api, docker.Config{PoolSize: 2})

	_, err := c.AcquireWarm(context.Background(), docker.ContainerSpec{Image: "bad:image"})
	assert.Error(t, err)
}

func TestReleaseContainer_NotFound(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{})

	err := c.ReleaseContainer("nonexistent-id")
	assert.ErrorIs(t, err, docker.ErrContainerNotFound)
}

func TestReapIdleContainers(t *testing.T) {
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
}

func TestReapIdleContainers_ReapError(t *testing.T) {
	api := newMockAPI()
	api.stopError = errors.New("failed to stop")
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
}

func TestStartReaper(t *testing.T) {
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
}

func TestClose(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{})

	err := c.Close()
	assert.NoError(t, err)
}

func TestHasImage_ListError(t *testing.T) {
	api := newMockAPI()
	api.listError = errors.New("docker daemon error")
	c := docker.NewClientWithAPI(api, docker.Config{})

	_, err := c.HasImage(context.Background(), "alpine:latest")
	assert.Error(t, err)
}

func TestReapIdleContainers_WithLogger_Success(t *testing.T) {
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
}

func TestReapIdleContainers_WithLogger_Error(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
	api := newMockAPI()
	api.stopError = errors.New("failed to stop")
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
}

func TestStartReaper_DefaultInterval(t *testing.T) {
	api := newMockAPI()
	c := docker.NewClientWithAPI(api, docker.Config{
		PoolSize:    2,
		IdleTimeout: 5 * time.Millisecond,
	})

	ctx, cancel := context.WithCancel(context.Background())
	// Pass 0 interval to use default (half of IdleTimeout).
	c.StartReaper(ctx, 0)

	cancel()
}
