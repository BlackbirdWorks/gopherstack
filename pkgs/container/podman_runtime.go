package container

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/docker/docker/client"
)

// socketProbeTimeout is the deadline for probing whether a container socket is reachable.
const socketProbeTimeout = 2 * time.Second

// podmanExtraCapacity is the number of extra slots reserved in podmanCandidateSockets
// for the CONTAINER_HOST and XDG_RUNTIME_DIR entries that are added dynamically.
const podmanExtraCapacity = 2

// podmanSocketPaths lists common fixed Podman socket locations for auto-detection.
// The first reachable socket wins. XDG_RUNTIME_DIR-based sockets are added dynamically
// in podmanCandidateSockets.
var podmanSocketPaths = []string{ //nolint:gochecknoglobals // intentional package-level constant slice
	// Common fixed paths for rootless and rootful Podman.
	"/run/user/1000/podman/podman.sock",
	"/run/podman/podman.sock",
	"/var/run/podman/podman.sock",
}

// podmanCandidateSockets returns the ordered list of Podman socket paths to probe.
func podmanCandidateSockets() []string {
	candidates := make([]string, 0, len(podmanSocketPaths)+podmanExtraCapacity)

	// Honour CONTAINER_HOST if set. This is a generic container endpoint override
	// and takes precedence over all auto-detection (XDG_RUNTIME_DIR and well-known paths).
	// Note: Docker auto-detection uses dockerSocketPaths and does not consult CONTAINER_HOST.
	if h := os.Getenv("CONTAINER_HOST"); h != "" {
		candidates = append(candidates, h)
	}

	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		candidates = append(candidates, "unix://"+xdg+"/podman/podman.sock")
	}

	for _, p := range podmanSocketPaths {
		if p != "" {
			candidates = append(candidates, "unix://"+p)
		}
	}

	return candidates
}

// dockerSocketPaths lists common Docker socket locations for auto-detection.
var dockerSocketPaths = []string{ //nolint:gochecknoglobals // intentional package-level constant slice
	"unix:///var/run/docker.sock",
	"unix:///run/docker.sock",
}

// newPodmanRuntime creates a DockerRuntime connected to the Podman socket.
// Podman exposes a Docker-compatible API so the same SDK and adapter work.
// The socket path is resolved via CONTAINER_HOST → XDG_RUNTIME_DIR → well-known paths.
// Returns ErrUnavailable if no reachable Podman socket is found.
func newPodmanRuntime(cfg Config) (*DockerRuntime, error) {
	candidates := podmanCandidateSockets()

	for _, addr := range candidates {
		if addr == "" {
			continue
		}

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

		if pingErr != nil {
			_ = sdkClient.Close()

			continue
		}

		return newDockerRuntimeWithAPI(&realDockerClient{c: sdkClient}, cfg), nil
	}

	return nil, fmt.Errorf("%w: no reachable Podman socket found (tried %v)", ErrUnavailable, candidates)
}
