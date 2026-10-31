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
	"io"
	"log/slog"
	"maps"
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
	Logger *slog.Logger
	// Injection overrides the env-derived DNS/CA injection for spawned containers.
	Injection   *Injection
	Runtime     RuntimeName
	PoolSize    int
	IdleTimeout time.Duration
}

// Spec holds the specification for creating a container.
type Spec struct {
	Image   string
	Name    string
	Network string
	// WorkingDir overrides the image's working directory; empty keeps it.
	WorkingDir string
	// User runs the container process as uid[:gid]; empty keeps the image's user.
	User string
	// GroupAdd lists supplementary group IDs for the container process.
	GroupAdd []string
	// OCIRuntime selects the daemon-registered OCI runtime (e.g. runsc); empty uses the daemon default.
	OCIRuntime string
	// Labels are extra container labels; see ResourceLabels. Identity labels are added by the runtime.
	Labels     map[string]string
	Env        []string
	Mounts     []string
	Cmd        []string
	Entrypoint []string
	Ports      []string
	Tmpfs      []string
	Privileged bool
	// Unmanaged skips the identity labels, for containers that must outlive this gopherstack (e.g. `start`).
	Unmanaged bool
	// MemoryBytes caps container memory; zero is unlimited.
	MemoryBytes int64
	// NanoCPUs caps CPU in billionths of a CPU; zero is unlimited.
	NanoCPUs int64
}

// ExecResult is the outcome of a command run inside a container.
type ExecResult struct {
	// Stdout and Stderr hold the demultiplexed output streams.
	Stdout string
	Stderr string
	// ExitCode is the command's exit status.
	ExitCode int
}

// Execer runs a command in a container.
type Execer interface {
	Exec(ctx context.Context, containerID string, cmd []string) (ExecResult, error)
}

// EnvExecer is implemented by runtimes that can set environment variables on an exec process.
type EnvExecer interface {
	ExecEnv(ctx context.Context, containerID string, cmd, env []string) (ExecResult, error)
}

// ExecWithEnv runs cmd with env (KEY=VALUE entries) set on the process, keeping secrets out of argv.
// It never falls back to argv: a runtime without EnvExecer yields ErrExecUnsupported.
func ExecWithEnv(ctx context.Context, x Execer, containerID string, cmd, env []string) (ExecResult, error) {
	if len(env) == 0 {
		return x.Exec(ctx, containerID, cmd)
	}

	ex, ok := x.(EnvExecer)
	if !ok {
		return ExecResult{}, ErrExecUnsupported
	}

	return ex.ExecEnv(ctx, containerID, cmd, env)
}

// StreamExecer is implemented by runtimes that can stream a command's output while it runs.
type StreamExecer interface {
	// ExecStream runs cmd, writing combined stdout and stderr to out as produced; it returns the exit status.
	ExecStream(ctx context.Context, containerID string, cmd []string, out io.Writer) (int, error)
}

// ErrNetworkUnsupported is returned when the underlying client cannot manage networks.
var ErrNetworkUnsupported = errors.New("container networks are not supported by this runtime client")

// NetworkRuntime is implemented by runtimes that can manage user-defined container networks.
type NetworkRuntime interface {
	// CreateNetwork creates a bridge network and returns its ID.
	CreateNetwork(ctx context.Context, name string) (string, error)
	// ConnectNetwork attaches a running container to the network.
	ConnectNetwork(ctx context.Context, networkID, containerID string) error
	// RemoveNetwork deletes the network.
	RemoveNetwork(ctx context.Context, networkID string) error
}

// ErrVolumeUnsupported is returned when the underlying client cannot manage volumes.
var ErrVolumeUnsupported = errors.New("container volumes are not supported by this runtime client")

// VolumeRuntime is implemented by runtimes that can manage named volumes.
type VolumeRuntime interface {
	// CreateVolume creates a labelled named volume; an existing one is reused.
	CreateVolume(ctx context.Context, name string, labels map[string]string) error
	// RemoveVolume deletes the volume; a missing one is not an error.
	RemoveVolume(ctx context.Context, name string) error
	// ListUnusedVolumes lists the volumes carrying label (key or key=value) that no container references and that
	// are older than olderThan.
	ListUnusedVolumes(ctx context.Context, label string, olderThan time.Duration) ([]string, error)
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
	// StopContainer stops a container without removing it, keeping its filesystem and published ports.
	StopContainer(ctx context.Context, containerID string) error
	// StartContainer starts a container previously stopped by StopContainer.
	StartContainer(ctx context.Context, containerID string) error
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

func (s Spec) launchLabels() map[string]string {
	if s.Unmanaged {
		return s.Labels
	}

	out := CurrentIdentity().Labels()
	maps.Copy(out, s.Labels)
	out[LabelManaged] = labelTrue

	return out
}
