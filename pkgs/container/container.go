// Package container provides a runtime-agnostic container integration layer for Gopherstack.
// It supports Docker and Podman (any OCI-compatible runtime) via a single env var switch:
// CONTAINER_RUNTIME=docker|podman|auto.
//
// The Runtime interface abstracts image management and container lifecycle operations
// (create/start/stop/remove), volume mounts, and a simple warm-container pool with
// configurable idle timeout.
package container

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// ErrUnavailable is returned when the container runtime is not available on the host.
var ErrUnavailable = errors.New("container runtime is not available")

// ErrContainerNotFound is returned when a requested container does not exist in the pool.
var ErrContainerNotFound = errors.New("container not found")

// ErrInvalidPort is returned when a Spec.Ports entry is not a valid [IP:]HOST:CONTAINER mapping.
var ErrInvalidPort = errors.New("invalid port mapping")

// ErrExecUnsupported is returned by Exec when the underlying client cannot run commands in a container.
var ErrExecUnsupported = errors.New("container exec is not supported by this runtime client")

// ErrPoolExhausted is returned when no warm container is available and the pool is full.
var ErrPoolExhausted = errors.New("container pool exhausted")

// RuntimeName identifies which container runtime to use.
type RuntimeName string

const (
	// RuntimeDocker selects the Docker daemon.
	RuntimeDocker RuntimeName = "docker"
	// RuntimePodman selects the Podman daemon via its Docker-compatible socket.
	RuntimePodman RuntimeName = "podman"
	// RuntimeAuto auto-detects the available runtime by probing sockets.
	RuntimeAuto RuntimeName = "auto"
)

// Config holds configuration for the container runtime layer.
type Config struct {
	Logger      *slog.Logger
	Runtime     RuntimeName
	PoolSize    int
	IdleTimeout time.Duration
}

// Spec holds the specification for creating a container.
type Spec struct {
	// Image is the container image reference.
	Image string
	// Name is an optional container name.
	Name string
	// Env is a list of environment variables in KEY=VALUE format.
	Env []string
	// Mounts is a list of bind-mount strings in HOST:CONTAINER[:OPTIONS] format.
	Mounts []string
	// Cmd overrides the image's default CMD.
	Cmd []string
	// Entrypoint overrides the image's default ENTRYPOINT.
	Entrypoint []string
	// Ports publishes TCP ports as [IP:]HOST:CONTAINER (e.g. "127.0.0.1:19092:9092"); no IP binds all interfaces.
	Ports []string
	// Tmpfs mounts an in-memory tmpfs at each listed container path.
	Tmpfs []string
	// Privileged runs the container with full host privileges (needed by k3s).
	Privileged bool
}

// ExecResult is the outcome of a command run inside a container.
type ExecResult struct {
	// Stdout and Stderr hold the demultiplexed output streams.
	Stdout string
	Stderr string
	// ExitCode is the command's exit status.
	ExitCode int
}

// PooledContainer tracks a container managed by the warm pool.
type PooledContainer struct {
	// LastUsed is the timestamp of the last time this container was used.
	LastUsed time.Time
	// ID is the container ID.
	ID string
	// Image is the image the container was started from.
	Image string
	// InUse indicates whether the container is currently handling an invocation.
	InUse bool
}

// Runtime is the container-runtime abstraction used throughout Gopherstack.
// Both DockerRuntime and PodmanRuntime implement this interface.
type Runtime interface {
	// Ping checks whether the container daemon is reachable.
	Ping(ctx context.Context) error
	// PullImage pulls the specified image from the registry.
	PullImage(ctx context.Context, imageRef string) error
	// HasImage reports whether the given image reference is already present locally.
	HasImage(ctx context.Context, imageRef string) (bool, error)
	// CreateAndStart creates a new container from spec and starts it.
	// Returns the container ID.
	CreateAndStart(ctx context.Context, spec Spec) (string, error)
	// StopAndRemove stops and removes a container.
	StopAndRemove(ctx context.Context, containerID string) error
	// Exec runs cmd inside a running container and waits for it to finish.
	// A non-zero exit status is reported in the result, not as an error.
	Exec(ctx context.Context, containerID string, cmd []string) (ExecResult, error)
	// AcquireWarm returns a warm container from the pool for the given image.
	AcquireWarm(ctx context.Context, spec Spec) (*PooledContainer, error)
	// ReleaseContainer marks a pooled container as idle.
	ReleaseContainer(containerID string) error
	// ReapIdleContainers stops and removes containers idle longer than IdleTimeout.
	ReapIdleContainers(ctx context.Context)
	// StartReaper launches a background goroutine that periodically reaps idle containers.
	StartReaper(ctx context.Context, interval time.Duration)
	// Close releases resources held by the runtime client.
	Close() error
}
