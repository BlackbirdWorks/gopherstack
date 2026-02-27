package container

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/docker/docker/client"
)

// podmanSocketPaths lists common Podman socket locations for auto-detection.
// The first readable socket wins.
var podmanSocketPaths = []string{
	// XDG_RUNTIME_DIR-based rootless socket (most common on modern Linux).
	// Evaluated at runtime via os.Getenv.
	"", // placeholder — filled in by podmanCandidateSockets
	// Common fixed paths for rootless and rootful Podman.
	"/run/user/1000/podman/podman.sock",
	"/run/podman/podman.sock",
	"/var/run/podman/podman.sock",
}

// podmanCandidateSockets returns the ordered list of Podman socket paths to probe.
func podmanCandidateSockets() []string {
	candidates := make([]string, 0, len(podmanSocketPaths))

	// Honour CONTAINER_HOST / DOCKER_HOST if set.
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
var dockerSocketPaths = []string{
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

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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
