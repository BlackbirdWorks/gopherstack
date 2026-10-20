package container

import (
	"context"
	"errors"
	"fmt"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
)

// ErrWaitFailed wraps an error the daemon reported while waiting.
var ErrWaitFailed = errors.New("container wait failed")

// ErrWaitUnsupported is returned by WaitExit when the underlying client cannot wait on a container.
var ErrWaitUnsupported = errors.New("container wait is not supported by this runtime client")

// ExitWaiter is implemented by runtimes that can report a container's exit status.
type ExitWaiter interface {
	// WaitExit blocks until the container stops and returns its exit code.
	WaitExit(ctx context.Context, containerID string) (int, error)
}

// WaitAPIClient is implemented by clients that can wait for a container to stop.
type WaitAPIClient interface {
	ContainerWait(
		ctx context.Context,
		containerID string,
		options dockercontainer.WaitOptions,
	) dockercontainer.WaitResult
}

func (r *realDockerClient) ContainerWait(
	ctx context.Context,
	containerID string,
	options dockercontainer.WaitOptions,
) dockercontainer.WaitResult {
	return r.c.ContainerWait(ctx, containerID, options)
}

// WaitExit blocks until the container is no longer running and returns its exit code.
func (r *DockerRuntime) WaitExit(ctx context.Context, containerID string) (int, error) {
	api, ok := r.docker.(WaitAPIClient)
	if !ok {
		return 0, ErrWaitUnsupported
	}

	res := api.ContainerWait(ctx, containerID, dockercontainer.WaitOptions{})

	select {
	case <-ctx.Done():
		return 0, fmt.Errorf("container wait %q: %w", containerID, ctx.Err())
	case err := <-res.Error:
		return 0, fmt.Errorf("container wait %q: %w", containerID, err)
	case resp := <-res.Result:
		if resp.Error != nil {
			return int(resp.StatusCode), fmt.Errorf("%w %q: %s", ErrWaitFailed, containerID, resp.Error.Message)
		}

		return int(resp.StatusCode), nil
	}
}
