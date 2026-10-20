package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	mobyclient "github.com/moby/moby/client"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	compatbuild "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/build"
	compatcontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	compatimage "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	compatnetwork "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/network"
)

// Opt aliases the Moby client option type.
type Opt = mobyclient.Opt

// FromEnv applies Docker-compatible client settings from the environment.
func FromEnv() Opt {
	return mobyclient.FromEnv
}

// Client adapts the Moby split client to the older Docker SDK surface used here.
type Client struct {
	inner *mobyclient.Client
}

// NewClientWithOpts constructs a compatibility client from Moby options.
func NewClientWithOpts(ops ...Opt) (*Client, error) {
	inner, err := mobyclient.New(ops...)
	if err != nil {
		return nil, err
	}

	return &Client{inner: inner}, nil
}

// WithAPIVersionNegotiation enables API version negotiation.
func WithAPIVersionNegotiation() Opt {
	return nil
}

// WithHost sets the daemon host.
func WithHost(host string) Opt {
	return mobyclient.WithHost(host)
}

// ImagePull pulls an image and returns a stream that must be closed by the caller.
func (c *Client) ImagePull(
	ctx context.Context,
	refStr string,
	options compatimage.PullOptions,
) (io.ReadCloser, error) {
	return c.inner.ImagePull(ctx, refStr, mobyclient.ImagePullOptions{
		All:          options.All,
		RegistryAuth: options.RegistryAuth,
	})
}

// ImageTag adds the target tag to the source image.
func (c *Client) ImageTag(ctx context.Context, source, target string) error {
	_, err := c.inner.ImageTag(ctx, mobyclient.ImageTagOptions{Source: source, Target: target})

	return err
}

// ImageList lists local images.
func (c *Client) ImageList(
	ctx context.Context,
	options compatimage.ListOptions,
) ([]compatimage.Summary, error) {
	result, err := c.inner.ImageList(ctx, mobyclient.ImageListOptions{
		All:        options.All,
		Filters:    options.Filters.Moby(),
		Identity:   options.Identity,
		Manifests:  options.Manifests,
		SharedSize: options.SharedSize,
	})
	if err != nil {
		return nil, err
	}

	return result.Items, nil
}

// ImageBuild builds an image from a tar context.
func (c *Client) ImageBuild(
	ctx context.Context,
	buildContext io.Reader,
	options compatbuild.ImageBuildOptions,
) (compatbuild.ImageBuildResponse, error) {
	result, err := c.inner.ImageBuild(ctx, buildContext, mobyclient.ImageBuildOptions{
		Dockerfile: options.Dockerfile,
		NoCache:    options.NoCache,
		PullParent: options.PullParent,
		Remove:     options.Remove,
		Tags:       options.Tags,
	})
	if err != nil {
		return compatbuild.ImageBuildResponse{}, err
	}

	return compatbuild.ImageBuildResponse{Body: result.Body}, nil
}

// ImageRemove removes an image from the daemon.
func (c *Client) ImageRemove(
	ctx context.Context,
	imageID string,
	options compatimage.RemoveOptions,
) ([]compatimage.DeleteResponse, error) {
	result, err := c.inner.ImageRemove(ctx, imageID, mobyclient.ImageRemoveOptions{
		Force:         options.Force,
		PruneChildren: options.PruneChildren,
	})
	if err != nil {
		return nil, err
	}

	return result.Items, nil
}

// ContainerCreate creates a container.
func (c *Client) ContainerCreate(
	ctx context.Context,
	cfg *compatcontainer.Config,
	hostConfig *compatcontainer.HostConfig,
	networkingConfig *compatnetwork.NetworkingConfig,
	platform *ocispec.Platform,
	containerName string,
) (compatcontainer.CreateResponse, error) {
	result, err := c.inner.ContainerCreate(ctx, mobyclient.ContainerCreateOptions{
		Config:           cfg,
		HostConfig:       hostConfig,
		Image:            "",
		Name:             containerName,
		NetworkingConfig: networkingConfig,
		Platform:         platform,
	})
	if err != nil {
		return compatcontainer.CreateResponse{}, err
	}

	return compatcontainer.CreateResponse{ID: result.ID, Warnings: result.Warnings}, nil
}

// ContainerStart starts a container.
func (c *Client) ContainerStart(
	ctx context.Context,
	containerID string,
	options compatcontainer.StartOptions,
) error {
	_, err := c.inner.ContainerStart(ctx, containerID, mobyclient.ContainerStartOptions{
		CheckpointDir: options.CheckpointDir,
		CheckpointID:  options.CheckpointID,
	})

	return err
}

// ContainerStop stops a container.
func (c *Client) ContainerStop(
	ctx context.Context,
	containerID string,
	options compatcontainer.StopOptions,
) error {
	_, err := c.inner.ContainerStop(ctx, containerID, mobyclient.ContainerStopOptions{
		Signal:  options.Signal,
		Timeout: options.Timeout,
	})

	return err
}

// ContainerRemove removes a container.
func (c *Client) ContainerRemove(
	ctx context.Context,
	containerID string,
	options compatcontainer.RemoveOptions,
) error {
	_, err := c.inner.ContainerRemove(ctx, containerID, mobyclient.ContainerRemoveOptions{
		Force:         options.Force,
		RemoveLinks:   options.RemoveLinks,
		RemoveVolumes: options.RemoveVolumes,
	})

	return err
}

// ContainerLogs returns a stream of a container's stdout/stderr. The caller
// must close it. Unless the container was created with a TTY, the stream is
// multiplexed per moby/moby/client's ContainerLogsOptions doc and must be
// demultiplexed with stdcopy.StdCopy before use.
func (c *Client) ContainerLogs(
	ctx context.Context,
	containerID string,
	options compatcontainer.LogsOptions,
) (io.ReadCloser, error) {
	return c.inner.ContainerLogs(ctx, containerID, mobyclient.ContainerLogsOptions{
		ShowStdout: options.ShowStdout,
		ShowStderr: options.ShowStderr,
		Since:      options.Since,
		Until:      options.Until,
		Timestamps: options.Timestamps,
		Follow:     options.Follow,
		Tail:       options.Tail,
		Details:    options.Details,
	})
}

// ContainerStats returns one JSON stats sample that includes the previous sample for CPU deltas. The caller must
// close it.
func (c *Client) ContainerStats(ctx context.Context, containerID string) (io.ReadCloser, error) {
	result, err := c.inner.ContainerStats(
		ctx,
		containerID,
		mobyclient.ContainerStatsOptions{IncludePreviousSample: true},
	)
	if err != nil {
		return nil, err
	}

	return result.Body, nil
}

// ContainerWait blocks until containerID satisfies options.Condition, matching
// github.com/moby/moby/client@v0.5.1 container_wait.go:41's
// (*Client).ContainerWait(ctx, containerID, ContainerWaitOptions)
// ContainerWaitResult, with the compat option/result types substituted in.
func (c *Client) ContainerWait(
	ctx context.Context,
	containerID string,
	options compatcontainer.WaitOptions,
) compatcontainer.WaitResult {
	result := c.inner.ContainerWait(ctx, containerID, mobyclient.ContainerWaitOptions{
		Condition: options.Condition,
	})

	return compatcontainer.WaitResult{Result: result.Result, Error: result.Error}
}

// ContainerList lists containers matching the provided filters.
func (c *Client) ContainerList(
	ctx context.Context,
	options compatcontainer.ListOptions,
) ([]compatcontainer.Summary, error) {
	result, err := c.inner.ContainerList(ctx, mobyclient.ContainerListOptions{
		All:     options.All,
		Filters: options.Filters.Moby(),
		Limit:   options.Limit,
		Size:    options.Size,
	})
	if err != nil {
		return nil, err
	}

	return result.Items, nil
}

// Ping checks daemon availability.
func (c *Client) Ping(ctx context.Context) (any, error) {
	_, err := c.inner.Ping(ctx, mobyclient.PingOptions{})
	if err != nil {
		return nil, err
	}

	return struct{}{}, nil
}

// Close releases the underlying client.
func (c *Client) Close() error {
	return c.inner.Close()
}

const execPollInterval = 50 * time.Millisecond

// ContainerExec runs cmd in a running container and returns its demultiplexed stdout, stderr and exit code.
func (c *Client) ContainerExec(
	ctx context.Context,
	containerID string,
	cmd []string,
) ([]byte, []byte, int, error) {
	var stdout, stderr bytes.Buffer

	code, err := c.ContainerExecStream(ctx, containerID, cmd, &stdout, &stderr)
	if err != nil {
		return nil, nil, 0, err
	}

	return stdout.Bytes(), stderr.Bytes(), code, nil
}

// ContainerExecStream runs cmd in a running container, writing demultiplexed output to the writers as it arrives.
func (c *Client) ContainerExecStream(
	ctx context.Context,
	containerID string,
	cmd []string,
	stdout, stderr io.Writer,
) (int, error) {
	created, err := c.inner.ExecCreate(ctx, containerID, mobyclient.ExecCreateOptions{
		AttachStdout: true,
		AttachStderr: true,
		Cmd:          cmd,
	})
	if err != nil {
		return 0, fmt.Errorf("exec create: %w", err)
	}

	attached, err := c.inner.ExecAttach(ctx, created.ID, mobyclient.ExecAttachOptions{})
	if err != nil {
		return 0, fmt.Errorf("exec attach: %w", err)
	}

	defer attached.Close()

	if _, err = stdcopy.StdCopy(stdout, stderr, attached.Reader); err != nil {
		return 0, fmt.Errorf("exec read: %w", err)
	}

	ticker := time.NewTicker(execPollInterval)
	defer ticker.Stop()

	for {
		info, ierr := c.inner.ExecInspect(ctx, created.ID, mobyclient.ExecInspectOptions{})
		if ierr != nil {
			return 0, fmt.Errorf("exec inspect: %w", ierr)
		}

		if !info.Running {
			return info.ExitCode, nil
		}

		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("exec wait: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

// ErrNoContainerIP is returned when a container has no network address.
var ErrNoContainerIP = errors.New("container has no network address")

// ContainerIP returns the first network IP address of a running container.
func (c *Client) ContainerIP(ctx context.Context, containerID string) (string, error) {
	res, err := c.inner.ContainerInspect(ctx, containerID, mobyclient.ContainerInspectOptions{})
	if err != nil {
		return "", fmt.Errorf("container inspect: %w", err)
	}

	if ns := res.Container.NetworkSettings; ns != nil {
		for _, ep := range ns.Networks {
			if ep != nil && ep.IPAddress.IsValid() {
				return ep.IPAddress.String(), nil
			}
		}
	}

	return "", fmt.Errorf("%w: %s", ErrNoContainerIP, containerID)
}

// InteractiveExec is a TTY exec attached to stdin and stdout.
type InteractiveExec struct {
	cli  *Client
	conn mobyclient.HijackedResponse
	ID   string
}

// ContainerExecInteractive starts cmd in a container with a TTY and attached stdin.
func (c *Client) ContainerExecInteractive(
	ctx context.Context,
	containerID string,
	cmd []string,
	cols, rows uint,
) (*InteractiveExec, error) {
	size := mobyclient.ConsoleSize{Height: rows, Width: cols}

	created, err := c.inner.ExecCreate(ctx, containerID, mobyclient.ExecCreateOptions{
		AttachStdin:  true,
		AttachStdout: true,
		AttachStderr: true,
		TTY:          true,
		ConsoleSize:  size,
		Cmd:          cmd,
	})
	if err != nil {
		return nil, fmt.Errorf("exec create: %w", err)
	}

	attached, err := c.inner.ExecAttach(ctx, created.ID, mobyclient.ExecAttachOptions{TTY: true, ConsoleSize: size})
	if err != nil {
		return nil, fmt.Errorf("exec attach: %w", err)
	}

	return &InteractiveExec{cli: c, conn: attached.HijackedResponse, ID: created.ID}, nil
}

// Read reads terminal output.
func (e *InteractiveExec) Read(p []byte) (int, error) { return e.conn.Reader.Read(p) }

// Write writes terminal input.
func (e *InteractiveExec) Write(p []byte) (int, error) { return e.conn.Conn.Write(p) }

// Close detaches from the exec; the shell exits when its TTY hangs up.
func (e *InteractiveExec) Close() error {
	e.conn.Close()

	return nil
}

// Resize changes the TTY size.
func (e *InteractiveExec) Resize(ctx context.Context, cols, rows uint) error {
	_, err := e.cli.inner.ExecResize(ctx, e.ID, mobyclient.ExecResizeOptions{Height: rows, Width: cols})

	return err
}
