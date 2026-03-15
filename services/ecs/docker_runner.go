package ecs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"

	dockertypes "github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	dockernetwork "github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// dockerClient is the subset of the Docker API used by realDockerRunner.
// It is defined as an interface to allow injection of fakes in tests.
type dockerClient interface {
	ImagePull(ctx context.Context, refStr string, options dockerimage.PullOptions) (io.ReadCloser, error)
	ContainerCreate(
		ctx context.Context,
		config *dockertypes.Config,
		hostConfig *dockertypes.HostConfig,
		networkingConfig *dockernetwork.NetworkingConfig,
		platform *ocispec.Platform,
		containerName string,
	) (dockertypes.CreateResponse, error)
	ContainerStart(ctx context.Context, containerID string, options dockertypes.StartOptions) error
	ContainerStop(ctx context.Context, containerID string, options dockertypes.StopOptions) error
	ContainerRemove(ctx context.Context, containerID string, options dockertypes.RemoveOptions) error
}

// NewDockerRunner creates a TaskRunner backed by the local Docker daemon.
// It uses the standard DOCKER_HOST / DOCKER_TLS_VERIFY environment variables
// via client.FromEnv, so it works both locally and inside docker-in-docker.
func NewDockerRunner() (TaskRunner, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return newDockerRunnerWithClient(cli), nil
}

// newDockerRunnerWithClient creates a realDockerRunner using the provided dockerClient.
// This constructor is used by tests to inject a fake Docker client.
func newDockerRunnerWithClient(cli dockerClient) *realDockerRunner {
	return &realDockerRunner{cli: cli, containers: make(map[string][]string)}
}

// realDockerRunner is a TaskRunner that launches Docker containers.
type realDockerRunner struct {
	containers map[string][]string
	cli        dockerClient
	mu         sync.Mutex
}

func (r *realDockerRunner) RunTask(task *Task, td *TaskDefinition) error {
	ctx := context.Background()
	log := logger.Load(ctx)

	// started accumulates container IDs that were successfully started during
	// this call.  If any later container fails we roll back by stopping and
	// removing everything accumulated so far, keeping RunTask atomic.
	var started []string

	for _, cd := range td.ContainerDefinitions {
		if err := r.pullImage(ctx, cd.Image); err != nil {
			r.rollbackContainers(ctx, started)

			return err
		}

		containerID, err := r.createContainer(ctx, task, cd)
		if err != nil {
			r.rollbackContainers(ctx, started)

			return err
		}

		if startErr := r.cli.ContainerStart(ctx, containerID, dockertypes.StartOptions{}); startErr != nil {
			// Clean up the just-created container before rolling back the rest.
			if rmErr := r.cli.ContainerRemove(ctx, containerID, dockertypes.RemoveOptions{Force: true}); rmErr != nil {
				log.WarnContext(ctx, "failed to remove container after start failure",
					"containerID", containerID,
					"error", rmErr,
				)
			}

			r.rollbackContainers(ctx, started)

			return fmt.Errorf("start container %s: %w", containerID, startErr)
		}

		started = append(started, containerID)
	}

	// All containers started successfully; register them in the tracking map.
	r.mu.Lock()
	r.containers[task.TaskArn] = append(r.containers[task.TaskArn], started...)
	r.mu.Unlock()

	return nil
}

// rollbackContainers stops and force-removes a set of already-started
// containers.  Errors are logged but not returned so that all containers are
// attempted.  This is called on the error path of RunTask to ensure the
// task leaves no running containers behind.
func (r *realDockerRunner) rollbackContainers(ctx context.Context, containerIDs []string) {
	log := logger.Load(ctx)
	timeout := 10

	for _, id := range containerIDs {
		if err := r.cli.ContainerStop(ctx, id, dockertypes.StopOptions{Timeout: &timeout}); err != nil {
			log.WarnContext(ctx, "failed to stop container during rollback", "containerID", id, "error", err)
		}

		if err := r.cli.ContainerRemove(ctx, id, dockertypes.RemoveOptions{Force: true}); err != nil {
			log.WarnContext(ctx, "failed to remove container during rollback", "containerID", id, "error", err)
		}
	}
}

// pullImage pulls a Docker image and drains the response body.
func (r *realDockerRunner) pullImage(ctx context.Context, image string) error {
	reader, err := r.cli.ImagePull(ctx, image, dockerimage.PullOptions{})
	if err != nil {
		return fmt.Errorf("pull image %s: %w", image, err)
	}

	if _, copyErr := io.Copy(io.Discard, reader); copyErr != nil {
		_ = reader.Close()

		return fmt.Errorf("drain image pull response for %s: %w", image, copyErr)
	}

	if closeErr := reader.Close(); closeErr != nil {
		return fmt.Errorf("close image pull reader for %s: %w", image, closeErr)
	}

	return nil
}

// createContainer creates a Docker container for the given container definition.
func (r *realDockerRunner) createContainer(ctx context.Context, task *Task, cd ContainerDefinition) (string, error) {
	portBindings, exposedPorts := buildPortMappings(cd.PortMappings)
	env := buildEnv(cd.Environment)

	resp, err := r.cli.ContainerCreate(
		ctx,
		&dockertypes.Config{
			Image:        cd.Image,
			Env:          env,
			ExposedPorts: exposedPorts,
			Labels: map[string]string{
				"gopherstack.ecs.task":    task.TaskArn,
				"gopherstack.ecs.cluster": task.ClusterArn,
			},
		},
		&dockertypes.HostConfig{
			PortBindings: portBindings,
		},
		nil,
		nil,
		"",
	)
	if err != nil {
		return "", fmt.Errorf("create container for %s: %w", cd.Image, err)
	}

	return resp.ID, nil
}

// buildPortMappings converts PortMappings to Docker nat.PortMap and nat.PortSet.
func buildPortMappings(mappings []PortMapping) (nat.PortMap, nat.PortSet) {
	portBindings := nat.PortMap{}
	exposedPorts := nat.PortSet{}

	for _, pm := range mappings {
		proto := pm.Protocol
		if proto == "" {
			proto = "tcp"
		}

		containerPort := nat.Port(fmt.Sprintf("%d/%s", pm.ContainerPort, proto))
		exposedPorts[containerPort] = struct{}{}

		if pm.HostPort > 0 {
			portBindings[containerPort] = []nat.PortBinding{
				{HostIP: "0.0.0.0", HostPort: strconv.Itoa(pm.HostPort)},
			}
		}
	}

	return portBindings, exposedPorts
}

// buildEnv converts KeyValuePairs to Docker-compatible "KEY=VALUE" strings.
func buildEnv(kvs []KeyValuePair) []string {
	env := make([]string, 0, len(kvs))
	for _, kv := range kvs {
		env = append(env, fmt.Sprintf("%s=%s", kv.Name, kv.Value))
	}

	return env
}

func (r *realDockerRunner) StopTask(task *Task) error {
	// Snapshot the container IDs while holding the lock but without removing
	// the entry yet — we only remove it once all stops have been attempted.
	r.mu.Lock()
	containerIDs := append([]string(nil), r.containers[task.TaskArn]...)
	r.mu.Unlock()

	if len(containerIDs) == 0 {
		return nil
	}

	ctx := context.Background()
	timeout := 10

	var (
		errs   []error
		failed []string
	)

	for _, containerID := range containerIDs {
		if err := r.cli.ContainerStop(ctx, containerID, dockertypes.StopOptions{Timeout: &timeout}); err != nil {
			errs = append(errs, fmt.Errorf("stop container %s: %w", containerID, err))
			failed = append(failed, containerID)
		}
	}

	// Update the tracking map: remove the entry entirely on full success, or
	// retain only the containers that could not be stopped so callers can retry.
	r.mu.Lock()

	if len(failed) == 0 {
		delete(r.containers, task.TaskArn)
	} else {
		r.containers[task.TaskArn] = failed
	}

	r.mu.Unlock()

	return errors.Join(errs...)
}

// newTaskRunner creates the appropriate TaskRunner based on the
// GOPHERSTACK_ECS_RUNTIME environment variable.
// Returns a no-op runner when the environment variable is absent or "none".
func newTaskRunner() (TaskRunner, error) {
	switch os.Getenv("GOPHERSTACK_ECS_RUNTIME") {
	case "docker":
		return NewDockerRunner()
	default:
		// "none" or unset – no-op
		return NewNoopRunner(), nil
	}
}
