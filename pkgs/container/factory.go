package container

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/docker/docker/client"
)

// ErrUnknownRuntime is returned when an unrecognised runtime name is provided.
var ErrUnknownRuntime = errors.New("unknown container runtime")

// NewRuntime creates a Runtime using the configured (or auto-detected) container runtime.
//
// The runtime is selected in the following order:
//  1. cfg.Runtime (if non-empty)
//  2. CONTAINER_RUNTIME environment variable
//  3. Defaults to RuntimeDocker
//
// RuntimeAuto probes the Docker socket first, then the Podman socket.
// Returns ErrUnavailable if the selected runtime is not reachable.
func NewRuntime(cfg Config) (Runtime, error) {
	name := cfg.Runtime
	if name == "" {
		if env := os.Getenv("CONTAINER_RUNTIME"); env != "" {
			name = RuntimeName(env)
		} else {
			name = RuntimeDocker
		}
	}

	switch name {
	case RuntimeDocker:
		return newDockerRuntime(cfg)
	case RuntimePodman:
		return newPodmanRuntime(cfg)
	case RuntimeAuto:
		return autoDetectRuntime(cfg)
	default:
		return nil, fmt.Errorf("%w %q; valid values: docker, podman, auto", ErrUnknownRuntime, name)
	}
}

// autoDetectRuntime probes Docker then Podman and returns the first reachable runtime.
func autoDetectRuntime(cfg Config) (Runtime, error) {
	// Try Docker first.
	for _, addr := range dockerSocketPaths {
		sdkClient, err := client.NewClientWithOpts(
			client.WithHost(addr),
			client.WithAPIVersionNegotiation(),
		)
		if err != nil {
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), socketProbeTimeout)
		_, pingErr := sdkClient.Ping(ctx)
		cancel()

		if pingErr == nil {
			return newDockerRuntimeWithAPI(&realDockerClient{c: sdkClient}, cfg), nil
		}

		_ = sdkClient.Close()
	}

	// Fall back to Podman.
	rt, err := newPodmanRuntime(cfg)
	if err == nil {
		return rt, nil
	}

	return nil, fmt.Errorf(
		"%w: neither Docker (tried %v) nor Podman socket is reachable",
		ErrUnavailable,
		dockerSocketPaths,
	)
}
