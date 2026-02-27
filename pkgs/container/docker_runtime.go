package container

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

// defaultIdleTimeout is the duration after which an idle container is reaped.
const defaultIdleTimeout = 10 * time.Minute

// defaultPoolSize is the default maximum warm containers per image.
const defaultPoolSize = 3

// reaperIntervalDivisor is the divisor used to compute the default reaper interval
// from the idle timeout. Reaper runs at half the idle timeout by default.
const reaperIntervalDivisor = 2

// minReaperInterval is the minimum allowed ticker interval to prevent a zero or
// negative duration being passed to [time.NewTicker] (which would panic).
const minReaperInterval = time.Millisecond

// stopTimeoutSecs is the timeout in seconds for stopping a container.
const stopTimeoutSecs = 10

// APIClient is a subset of the Docker/Podman SDK client interface used by DockerRuntime.
// It is defined as an interface to enable testing without a real container daemon.
type APIClient interface {
	ImagePull(ctx context.Context, refStr string, options image.PullOptions) (io.ReadCloser, error)
	ImageList(ctx context.Context, options image.ListOptions) ([]image.Summary, error)
	ContainerCreate(
		ctx context.Context,
		cfg *dockercontainer.Config,
		hostConfig *dockercontainer.HostConfig,
		networkingConfig any,
		platform any,
		containerName string,
	) (dockercontainer.CreateResponse, error)
	ContainerStart(ctx context.Context, containerID string, options dockercontainer.StartOptions) error
	ContainerStop(ctx context.Context, containerID string, options dockercontainer.StopOptions) error
	ContainerRemove(ctx context.Context, containerID string, options dockercontainer.RemoveOptions) error
	Ping(ctx context.Context) (any, error)
	Close() error
}

// realDockerClient wraps the standard Docker SDK client to satisfy APIClient.
type realDockerClient struct {
	c *client.Client
}

func (r *realDockerClient) ImagePull(
	ctx context.Context,
	refStr string,
	options image.PullOptions,
) (io.ReadCloser, error) {
	return r.c.ImagePull(ctx, refStr, options)
}

func (r *realDockerClient) ImageList(ctx context.Context, options image.ListOptions) ([]image.Summary, error) {
	return r.c.ImageList(ctx, options)
}

func (r *realDockerClient) ContainerCreate(
	ctx context.Context,
	cfg *dockercontainer.Config,
	hostConfig *dockercontainer.HostConfig,
	_ any,
	_ any,
	containerName string,
) (dockercontainer.CreateResponse, error) {
	return r.c.ContainerCreate(ctx, cfg, hostConfig, nil, nil, containerName)
}

func (r *realDockerClient) ContainerStart(
	ctx context.Context,
	containerID string,
	options dockercontainer.StartOptions,
) error {
	return r.c.ContainerStart(ctx, containerID, options)
}

func (r *realDockerClient) ContainerStop(ctx context.Context, containerID string, options dockercontainer.StopOptions) error {
	return r.c.ContainerStop(ctx, containerID, options)
}

func (r *realDockerClient) ContainerRemove(
	ctx context.Context,
	containerID string,
	options dockercontainer.RemoveOptions,
) error {
	return r.c.ContainerRemove(ctx, containerID, options)
}

func (r *realDockerClient) Ping(ctx context.Context) (any, error) {
	return r.c.Ping(ctx)
}

func (r *realDockerClient) Close() error {
	return r.c.Close()
}

// DockerRuntime implements Runtime using the Docker daemon.
type DockerRuntime struct {
	docker APIClient
	pools  map[string][]*PooledContainer
	cfg    Config
	mu     sync.Mutex
}

// newDockerRuntime creates a DockerRuntime connected to the Docker daemon.
// Returns ErrUnavailable if Docker is not reachable.
func newDockerRuntime(cfg Config) (*DockerRuntime, error) {
	sdkClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}

	return newDockerRuntimeWithAPI(&realDockerClient{c: sdkClient}, cfg), nil
}

// NewDockerRuntimeWithAPI creates a DockerRuntime with an injected APIClient.
// This is primarily intended for testing; production code should use NewRuntime.
func NewDockerRuntimeWithAPI(api APIClient, cfg Config) *DockerRuntime {
	return newDockerRuntimeWithAPI(api, cfg)
}

// newDockerRuntimeWithAPI creates a DockerRuntime with an injected APIClient.
func newDockerRuntimeWithAPI(api APIClient, cfg Config) *DockerRuntime {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = defaultPoolSize
	}

	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}

	return &DockerRuntime{
		pools:  make(map[string][]*PooledContainer),
		cfg:    cfg,
		docker: api,
	}
}

// Ping checks whether the Docker daemon is reachable.
func (r *DockerRuntime) Ping(ctx context.Context) error {
	_, err := r.docker.Ping(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}

	return nil
}

// PullImage pulls the specified image from the registry.
func (r *DockerRuntime) PullImage(ctx context.Context, imageRef string) error {
	rc, err := r.docker.ImagePull(ctx, imageRef, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("image pull %q: %w", imageRef, err)
	}
	defer rc.Close()

	_, _ = io.Copy(io.Discard, rc)

	return nil
}

// HasImage reports whether the given image reference is already present locally.
func (r *DockerRuntime) HasImage(ctx context.Context, imageRef string) (bool, error) {
	filter := filters.NewArgs(filters.KeyValuePair{Key: "reference", Value: imageRef})
	images, err := r.docker.ImageList(ctx, image.ListOptions{Filters: filter})

	if err != nil {
		return false, fmt.Errorf("image list: %w", err)
	}

	return len(images) > 0, nil
}

// CreateAndStart creates a new container from spec and starts it.
// Returns the container ID.
func (r *DockerRuntime) CreateAndStart(ctx context.Context, spec ContainerSpec) (string, error) {
	cfg := &dockercontainer.Config{
		Image: spec.Image,
		Env:   spec.Env,
	}

	if len(spec.Cmd) > 0 {
		cfg.Cmd = spec.Cmd
	}

	hostCfg := &dockercontainer.HostConfig{
		Binds: spec.Mounts,
	}

	resp, err := r.docker.ContainerCreate(ctx, cfg, hostCfg, nil, nil, spec.Name)
	if err != nil {
		return "", fmt.Errorf("container create %q: %w", spec.Image, err)
	}

	if startErr := r.docker.ContainerStart(ctx, resp.ID, dockercontainer.StartOptions{}); startErr != nil {
		return "", fmt.Errorf("container start %q: %w", resp.ID, startErr)
	}

	return resp.ID, nil
}

// StopAndRemove stops and removes a container.
func (r *DockerRuntime) StopAndRemove(ctx context.Context, containerID string) error {
	timeout := stopTimeoutSecs

	if err := r.docker.ContainerStop(ctx, containerID, dockercontainer.StopOptions{Timeout: &timeout}); err != nil {
		return fmt.Errorf("container stop %q: %w", containerID, err)
	}

	if err := r.docker.ContainerRemove(ctx, containerID, dockercontainer.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("container remove %q: %w", containerID, err)
	}

	return nil
}

// AcquireWarm returns a warm container from the pool for the given image.
// If no warm container is available, a new one is created (up to PoolSize).
// The caller must call ReleaseContainer when done.
func (r *DockerRuntime) AcquireWarm(ctx context.Context, spec ContainerSpec) (*PooledContainer, error) {
	r.mu.Lock()

	pool := r.pools[spec.Image]
	for _, pc := range pool {
		if !pc.InUse {
			pc.InUse = true
			pc.LastUsed = time.Now()
			r.mu.Unlock()

			return pc, nil
		}
	}

	if len(pool) >= r.cfg.PoolSize {
		r.mu.Unlock()

		return nil, ErrPoolExhausted
	}

	r.mu.Unlock()

	id, err := r.CreateAndStart(ctx, spec)
	if err != nil {
		return nil, err
	}

	pc := &PooledContainer{
		ID:       id,
		Image:    spec.Image,
		LastUsed: time.Now(),
		InUse:    true,
	}

	r.mu.Lock()
	r.pools[spec.Image] = append(r.pools[spec.Image], pc)
	r.mu.Unlock()

	return pc, nil
}

// ReleaseContainer marks a pooled container as idle.
// If the container is not in the pool, ErrContainerNotFound is returned.
func (r *DockerRuntime) ReleaseContainer(containerID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, pool := range r.pools {
		for _, pc := range pool {
			if pc.ID == containerID {
				pc.InUse = false
				pc.LastUsed = time.Now()

				return nil
			}
		}
	}

	return fmt.Errorf("%w: %s", ErrContainerNotFound, containerID)
}

// ReapIdleContainers stops and removes containers that have been idle longer than IdleTimeout.
func (r *DockerRuntime) ReapIdleContainers(ctx context.Context) {
	r.mu.Lock()
	toReap := r.collectIdleLocked()
	r.mu.Unlock()

	for _, entry := range toReap {
		if err := r.StopAndRemove(ctx, entry.ID); err != nil {
			if r.cfg.Logger != nil {
				r.cfg.Logger.WarnContext(ctx, "container: failed to reap idle container",
					"id", entry.ID, "image", entry.Image, "error", err)
			}
		} else {
			if r.cfg.Logger != nil {
				r.cfg.Logger.DebugContext(ctx, "container: reaped idle container", "id", entry.ID, "image", entry.Image)
			}
		}
	}
}

// collectIdleLocked identifies idle containers to reap and removes them from the pools map.
// Must be called with r.mu held.
func (r *DockerRuntime) collectIdleLocked() []*PooledContainer {
	deadline := time.Now().Add(-r.cfg.IdleTimeout)
	var toReap []*PooledContainer

	for img, pool := range r.pools {
		var keep []*PooledContainer

		for _, pc := range pool {
			if !pc.InUse && pc.LastUsed.Before(deadline) {
				toReap = append(toReap, pc)
			} else {
				keep = append(keep, pc)
			}
		}

		r.pools[img] = keep
	}

	return toReap
}

// StartReaper launches a background goroutine that periodically reaps idle containers.
// It stops when ctx is cancelled.
func (r *DockerRuntime) StartReaper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = r.cfg.IdleTimeout / reaperIntervalDivisor
	}

	if interval < minReaperInterval {
		interval = minReaperInterval
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.ReapIdleContainers(ctx)
			}
		}
	}()
}

// Close releases resources held by the Docker client.
func (r *DockerRuntime) Close() error {
	return r.docker.Close()
}
