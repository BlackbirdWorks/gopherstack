package container

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/moby/moby/api/pkg/stdcopy"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
)

// ErrLogsUnsupported is returned by StreamLogs when the underlying client cannot read container logs.
var ErrLogsUnsupported = errors.New("container logs are not supported by this runtime client")

// LogStreamer is implemented by runtimes that can follow a container's stdout and stderr.
type LogStreamer interface {
	// StreamLogs writes the container's stdout and stderr to the given writers as they are produced.
	// It blocks until the container is removed or stops, or ctx ends.
	StreamLogs(ctx context.Context, containerID string, stdout, stderr io.Writer) error
}

// LogsAPIClient is implemented by clients that can read a container's logs.
type LogsAPIClient interface {
	ContainerLogs(ctx context.Context, containerID string, options dockercontainer.LogsOptions) (io.ReadCloser, error)
}

func (r *realDockerClient) ContainerLogs(
	ctx context.Context,
	containerID string,
	options dockercontainer.LogsOptions,
) (io.ReadCloser, error) {
	return r.c.ContainerLogs(ctx, containerID, options)
}

// StreamLogs follows the container's logs, demultiplexing stdout and stderr into the writers.
func (r *DockerRuntime) StreamLogs(ctx context.Context, containerID string, stdout, stderr io.Writer) error {
	api, ok := r.docker.(LogsAPIClient)
	if !ok {
		return ErrLogsUnsupported
	}

	rc, err := api.ContainerLogs(ctx, containerID, dockercontainer.LogsOptions{
		ShowStdout: true,
		ShowStderr: true,
		Follow:     true,
	})
	if err != nil {
		return fmt.Errorf("container logs %q: %w", containerID, err)
	}
	defer rc.Close()

	if _, err = stdcopy.StdCopy(stdout, stderr, rc); err != nil && ctx.Err() == nil {
		return fmt.Errorf("container logs %q: %w", containerID, err)
	}

	return nil
}
