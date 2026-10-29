package ssm

import (
	"context"
	"fmt"
	"io"

	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
)

// InteractiveExecAPI starts a TTY exec with attached stdin.
type InteractiveExecAPI interface {
	io.Closer
	ContainerExecInteractive(
		ctx context.Context,
		containerID string,
		cmd []string,
		cols, rows uint,
	) (*client.InteractiveExec, error)
}

type dockerShellOpener struct{ api InteractiveExecAPI }

// NewDockerShellOpener returns a ShellOpener backed by the Docker daemon selected by DOCKER_HOST.
func NewDockerShellOpener() (ShellOpener, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return &dockerShellOpener{api: cli}, nil
}

func (d *dockerShellOpener) OpenShell(ctx context.Context, containerID string, cols, rows uint) (Shell, error) {
	return d.api.ContainerExecInteractive(
		context.WithoutCancel(ctx), containerID,
		[]string{"sh", "-c", "command -v bash >/dev/null 2>&1 && exec bash -l; exec sh -l"},
		cols, rows,
	)
}

// OpenCommand runs command through sh in the container with a TTY.
func (d *dockerShellOpener) OpenCommand(
	ctx context.Context, containerID string, cmd []string, cols, rows uint,
) (Shell, error) {
	return d.api.ContainerExecInteractive(context.WithoutCancel(ctx), containerID, cmd, cols, rows)
}

// Close releases the Docker client.
func (d *dockerShellOpener) Close() error { return d.api.Close() }
