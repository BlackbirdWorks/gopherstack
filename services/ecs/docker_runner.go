package ecs

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"

	dockertypes "github.com/docker/docker/api/types/container"
	dockerimage "github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
)

// NewDockerRunner creates a TaskRunner backed by the local Docker daemon.
// It uses the standard DOCKER_HOST / DOCKER_TLS_VERIFY environment variables
// via client.FromEnv, so it works both locally and inside docker-in-docker.
func NewDockerRunner() (TaskRunner, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return &realDockerRunner{cli: cli, containers: make(map[string]string)}, nil
}

// realDockerRunner is a TaskRunner that launches Docker containers.
type realDockerRunner struct {
	containers map[string]string // taskArn -> containerID
	cli        *client.Client
}

func (r *realDockerRunner) RunTask(task *Task, td *TaskDefinition) error {
	ctx := context.Background()

	for _, cd := range td.ContainerDefinitions {
		// Pull the image.
		reader, err := r.cli.ImagePull(ctx, cd.Image, dockerimage.PullOptions{})
		if err != nil {
			return fmt.Errorf("pull image %s: %w", cd.Image, err)
		}

		_, _ = io.Copy(io.Discard, reader)
		_ = reader.Close()

		// Build port bindings from PortMappings.
		portBindings := nat.PortMap{}
		exposedPorts := nat.PortSet{}

		for _, pm := range cd.PortMappings {
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

		// Build environment variables.
		env := make([]string, 0, len(cd.Environment))
		for _, kv := range cd.Environment {
			env = append(env, fmt.Sprintf("%s=%s", kv.Name, kv.Value))
		}

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
			return fmt.Errorf("create container for %s: %w", cd.Image, err)
		}

		if startErr := r.cli.ContainerStart(ctx, resp.ID, dockertypes.StartOptions{}); startErr != nil {
			return fmt.Errorf("start container %s: %w", resp.ID, startErr)
		}

		r.containers[task.TaskArn] = resp.ID
	}

	return nil
}

func (r *realDockerRunner) StopTask(task *Task) error {
	containerID, ok := r.containers[task.TaskArn]
	if !ok {
		return nil
	}

	ctx := context.Background()
	timeout := 10

	if err := r.cli.ContainerStop(ctx, containerID, dockertypes.StopOptions{Timeout: &timeout}); err != nil {
		return fmt.Errorf("stop container %s: %w", containerID, err)
	}

	delete(r.containers, task.TaskArn)

	return nil
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
