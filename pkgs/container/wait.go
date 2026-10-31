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

	resp, err := AwaitWait(ctx, res)
	if err != nil {
		return 0, fmt.Errorf("container wait %q: %w", containerID, err)
	}

	if resp.Error != nil {
		return int(resp.StatusCode), fmt.Errorf("%w %q: %s", ErrWaitFailed, containerID, resp.Error.Message)
	}

	return int(resp.StatusCode), nil
}

// AwaitWait blocks for the first of res.Result, res.Error or ctx cancellation.
// Moby's resultC is unbuffered and its sender never selects on ctx, so on early
// return a drainer receives from res to let that goroutine exit: it either sends
// the result or, once ctx is cancelled, fails the body read into buffered errC.
func AwaitWait(ctx context.Context, res dockercontainer.WaitResult) (dockercontainer.WaitResponse, error) {
	select {
	case resp := <-res.Result:
		return resp, nil
	case err := <-res.Error:
		return dockercontainer.WaitResponse{}, err
	case <-ctx.Done():
		go func() {
			select {
			case <-res.Result:
			case <-res.Error:
			}
		}()

		return dockercontainer.WaitResponse{}, ctx.Err()
	}
}
