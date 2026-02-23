// Package docker provides a Docker integration layer for Gopherstack.
// It wraps the Docker SDK to support image pulling, container lifecycle
// management (create/start/stop/remove), volume mounts, and a simple
// warm-container pool with configurable idle timeout.
//
// This package is the foundation for Lambda runtime container execution
// (Python, Node.js, Java, .NET, Ruby, custom images) planned for v0.7.
package docker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
)

// ErrDockerUnavailable is returned when Docker is not available on the host.
var ErrDockerUnavailable = errors.New("docker daemon is not available")

// ErrContainerNotFound is returned when a requested container does not exist in the pool.
var ErrContainerNotFound = errors.New("container not found")

// ErrPoolExhausted is returned when no warm container is available and the pool is full.
var ErrPoolExhausted = errors.New("container pool exhausted")

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

// Config holds configuration for the Docker integration layer.
type Config struct {
	// Logger is an optional structured logger.
	Logger *slog.Logger
	// PoolSize is the maximum number of warm containers per image.
	// Defaults to 3.
	PoolSize int
	// IdleTimeout is the duration after which an idle container is reaped.
	// Defaults to 10 minutes.
	IdleTimeout time.Duration
}

// PooledContainer tracks a container managed by the warm pool.
type PooledContainer struct {
	// LastUsed is the timestamp of the last time this container was used.
	LastUsed time.Time
	// ID is the Docker container ID.
	ID string
	// Image is the image the container was started from.
	Image string
	// InUse indicates whether the container is currently handling an invocation.
	InUse bool
}

// Client is the Gopherstack Docker client. It wraps the Docker SDK and provides
// image management and a per-image warm container pool.
type Client struct {
	docker APIClient
	pools  map[string][]*PooledContainer
	cfg    Config
	mu     sync.Mutex
}

// APIClient is a subset of the Docker SDK client interface used by this package.
// It is defined as an interface to enable testing without a real Docker daemon.
type APIClient interface {
	ImagePull(ctx context.Context, refStr string, options image.PullOptions) (io.ReadCloser, error)
	ImageList(ctx context.Context, options image.ListOptions) ([]image.Summary, error)
	ContainerCreate(
		ctx context.Context,
		cfg *container.Config,
		hostConfig *container.HostConfig,
		networkingConfig any,
		platform any,
		containerName string,
	) (container.CreateResponse, error)
	ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error
	ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error
	ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error
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
	cfg *container.Config,
	hostConfig *container.HostConfig,
	_ any,
	_ any,
	containerName string,
) (container.CreateResponse, error) {
	return r.c.ContainerCreate(ctx, cfg, hostConfig, nil, nil, containerName)
}

func (r *realDockerClient) ContainerStart(
	ctx context.Context,
	containerID string,
	options container.StartOptions,
) error {
	return r.c.ContainerStart(ctx, containerID, options)
}

func (r *realDockerClient) ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error {
	return r.c.ContainerStop(ctx, containerID, options)
}

func (r *realDockerClient) ContainerRemove(
	ctx context.Context,
	containerID string,
	options container.RemoveOptions,
) error {
	return r.c.ContainerRemove(ctx, containerID, options)
}

func (r *realDockerClient) Ping(ctx context.Context) (any, error) {
	return r.c.Ping(ctx)
}

func (r *realDockerClient) Close() error {
	return r.c.Close()
}

// NewClient creates a new Docker Client using the host's Docker daemon.
// Returns ErrDockerUnavailable if Docker is not reachable.
func NewClient(cfg Config) (*Client, error) {
	sdkClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrDockerUnavailable, err)
	}

	return NewClientWithAPI(&realDockerClient{c: sdkClient}, cfg), nil
}

// NewClientWithAPI creates a Client with an injected APIClient.
// This is primarily intended for testing; production code should use NewClient.
func NewClientWithAPI(api APIClient, cfg Config) *Client {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = defaultPoolSize
	}

	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}

	return &Client{
		pools:  make(map[string][]*PooledContainer),
		cfg:    cfg,
		docker: api,
	}
}

// Ping checks whether the Docker daemon is reachable.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.docker.Ping(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrDockerUnavailable, err)
	}

	return nil
}

// PullImage pulls the specified image from the registry.
// The pull output is discarded; callers that need progress should use the
// Docker SDK directly.
func (c *Client) PullImage(ctx context.Context, imageRef string) error {
	rc, err := c.docker.ImagePull(ctx, imageRef, image.PullOptions{})
	if err != nil {
		return fmt.Errorf("image pull %q: %w", imageRef, err)
	}
	defer rc.Close()

	// Drain the response to allow Docker to complete the pull.
	_, _ = io.Copy(io.Discard, rc)

	return nil
}

// HasImage reports whether the given image reference is already present locally.
func (c *Client) HasImage(ctx context.Context, imageRef string) (bool, error) {
	filter := filters.NewArgs(filters.KeyValuePair{Key: "reference", Value: imageRef})
	images, err := c.docker.ImageList(ctx, image.ListOptions{Filters: filter})

	if err != nil {
		return false, fmt.Errorf("image list: %w", err)
	}

	return len(images) > 0, nil
}

// ContainerSpec holds the specification for creating a container.
type ContainerSpec struct {
	// Image is the Docker image reference.
	Image string
	// Name is an optional container name.
	Name string
	// Env is a list of environment variables in KEY=VALUE format.
	Env []string
	// Mounts is a list of bind-mount strings in HOST:CONTAINER[:OPTIONS] format.
	Mounts []string
	// Cmd overrides the image's default CMD.
	Cmd []string
}

// CreateAndStart creates a new container from spec and starts it.
// Returns the container ID.
func (c *Client) CreateAndStart(ctx context.Context, spec ContainerSpec) (string, error) {
	cfg := &container.Config{
		Image: spec.Image,
		Env:   spec.Env,
	}

	if len(spec.Cmd) > 0 {
		cfg.Cmd = spec.Cmd
	}

	hostCfg := &container.HostConfig{
		Binds: spec.Mounts,
	}

	resp, err := c.docker.ContainerCreate(ctx, cfg, hostCfg, nil, nil, spec.Name)
	if err != nil {
		return "", fmt.Errorf("container create %q: %w", spec.Image, err)
	}

	if startErr := c.docker.ContainerStart(ctx, resp.ID, container.StartOptions{}); startErr != nil {
		return "", fmt.Errorf("container start %q: %w", resp.ID, startErr)
	}

	return resp.ID, nil
}

// StopAndRemove stops and removes a container.
func (c *Client) StopAndRemove(ctx context.Context, containerID string) error {
	stopTimeout := 10 // seconds

	if err := c.docker.ContainerStop(ctx, containerID, container.StopOptions{Timeout: &stopTimeout}); err != nil {
		return fmt.Errorf("container stop %q: %w", containerID, err)
	}

	if err := c.docker.ContainerRemove(ctx, containerID, container.RemoveOptions{Force: true}); err != nil {
		return fmt.Errorf("container remove %q: %w", containerID, err)
	}

	return nil
}

// AcquireWarm returns a warm container from the pool for the given image.
// If no warm container is available, a new one is created (up to PoolSize).
// The caller must call ReleaseContainer when done.
//
// Note: pool capacity is a soft limit. When multiple goroutines call AcquireWarm
// concurrently, the pool may temporarily exceed PoolSize by the number of
// concurrent callers that see no idle container and concurrently create new ones.
func (c *Client) AcquireWarm(ctx context.Context, spec ContainerSpec) (*PooledContainer, error) {
	// Check for an idle container first (fast path, lock held briefly).
	c.mu.Lock()

	pool := c.pools[spec.Image]
	for _, pc := range pool {
		if !pc.InUse {
			pc.InUse = true
			pc.LastUsed = time.Now()
			c.mu.Unlock()

			return pc, nil
		}
	}

	// Pool is full → return error.
	if len(pool) >= c.cfg.PoolSize {
		c.mu.Unlock()

		return nil, ErrPoolExhausted
	}

	c.mu.Unlock()

	// Create a new container outside the lock (Docker calls can be slow).
	id, err := c.CreateAndStart(ctx, spec)
	if err != nil {
		return nil, err
	}

	pc := &PooledContainer{
		ID:       id,
		Image:    spec.Image,
		LastUsed: time.Now(),
		InUse:    true,
	}

	c.mu.Lock()
	c.pools[spec.Image] = append(c.pools[spec.Image], pc)
	c.mu.Unlock()

	return pc, nil
}

// ReleaseContainer marks a pooled container as idle.
// If the container is not in the pool, ErrContainerNotFound is returned.
func (c *Client) ReleaseContainer(containerID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, pool := range c.pools {
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
// It is intended to be called periodically by a background goroutine.
func (c *Client) ReapIdleContainers(ctx context.Context) {
	c.mu.Lock()
	toReap := c.collectIdleLocked()
	c.mu.Unlock()

	for _, entry := range toReap {
		if err := c.StopAndRemove(ctx, entry.ID); err != nil {
			if c.cfg.Logger != nil {
				c.cfg.Logger.WarnContext(ctx, "docker: failed to reap idle container",
					"id", entry.ID, "image", entry.Image, "error", err)
			}
		} else {
			if c.cfg.Logger != nil {
				c.cfg.Logger.DebugContext(ctx, "docker: reaped idle container", "id", entry.ID, "image", entry.Image)
			}
		}
	}
}

// collectIdleLocked identifies idle containers to reap and removes them from the pools map.
// Must be called with c.mu held.
func (c *Client) collectIdleLocked() []*PooledContainer {
	deadline := time.Now().Add(-c.cfg.IdleTimeout)
	var toReap []*PooledContainer

	for img, pool := range c.pools {
		var keep []*PooledContainer

		for _, pc := range pool {
			if !pc.InUse && pc.LastUsed.Before(deadline) {
				toReap = append(toReap, pc)
			} else {
				keep = append(keep, pc)
			}
		}

		c.pools[img] = keep
	}

	return toReap
}

// StartReaper launches a background goroutine that periodically reaps idle containers.
// It stops when ctx is cancelled.
func (c *Client) StartReaper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = c.cfg.IdleTimeout / reaperIntervalDivisor
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
				c.ReapIdleContainers(ctx)
			}
		}
	}()
}

// Close releases resources held by the Docker client.
func (c *Client) Close() error {
	return c.docker.Close()
}
