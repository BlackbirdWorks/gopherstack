package batch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"sync"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	dockerimage "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	dockernetwork "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/network"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/docker"
)

const (
	bytesPerMiB        = 1 << 20
	dockerMinMemoryMiB = 6
	cpuSharesPerVCPU   = 1024
	stopGraceSeconds   = 2
	cleanupTimeout     = 30 * time.Second
	reasonMaxLen       = 255

	reasonPullFailed   = "CannotPullContainerError"
	reasonCreateFailed = "CannotCreateContainerError"
	reasonStartFailed  = "CannotStartContainerError"
	reasonEssentialEnd = "Essential container in task exited"
	reasonHostLost     = "Host EC2 terminated"
	reasonTimeout      = "Job attempt duration exceeded timeout"
)

// ContainerSpec is one job attempt's container.
type ContainerSpec struct {
	Labels         map[string]string
	Image          string
	User           string
	Network        string
	Command        []string
	Env            []string
	MemoryMiB      int64
	VCPUs          float64
	Privileged     bool
	ReadonlyRootFS bool
}

// RunHooks lets a ContainerRunner report progress while a container runs.
type RunHooks struct {
	// OnAddress reports the container's IPv4 address on ContainerSpec.Network, before OnStarted.
	OnAddress func(ip string)
	OnStarted func()
	OnLog     func(lines []string)
}

// ContainerRunner runs one container to completion. Cancelling ctx stops and
// removes the container and returns ctx.Err() wrapped.
type ContainerRunner interface {
	RunContainer(ctx context.Context, spec ContainerSpec, hooks RunHooks) (exitCode int, err error)
}

// JobNetworker is implemented by runners that can isolate one job's containers on a private network.
type JobNetworker interface {
	CreateJobNetwork(ctx context.Context, name string) (remove func(), err error)
}

// JobLogSink receives job container output. CloudWatch Logs is resolved by the
// implementation at call time.
type JobLogSink interface {
	EnsureLogGroupAndStream(region, group, stream string) error
	PutLogLines(region, group, stream string, messages []string) error
}

// JobCredentials are short-lived credentials for a job role.
type JobCredentials struct {
	Expires         time.Time
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// JobRoleCredentialIssuer assumes a job role for a container.
type JobRoleCredentialIssuer interface {
	IssueJobCredentials(roleArn, sessionName string) (JobCredentials, error)
}

type dockerAPI interface {
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
	ContainerLogs(ctx context.Context, containerID string, options dockertypes.LogsOptions) (io.ReadCloser, error)
	ContainerWait(ctx context.Context, containerID string, options dockertypes.WaitOptions) dockertypes.WaitResult
}

type networkAPI interface {
	NetworkCreate(ctx context.Context, name string) (string, error)
	NetworkRemove(ctx context.Context, networkID string) error
	ContainerIP(ctx context.Context, containerID string) (string, error)
}

type closer interface{ Close() error }

type dockerContainerRunner struct {
	cli dockerAPI
}

// NewDockerContainerRunner returns a ContainerRunner backed by the Docker daemon
// selected by the standard DOCKER_HOST environment.
func NewDockerContainerRunner() (ContainerRunner, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return &dockerContainerRunner{cli: cli}, nil
}

// Close releases the Docker client's idle connections.
func (r *dockerContainerRunner) Close() error {
	if c, ok := r.cli.(closer); ok {
		return c.Close()
	}

	return nil
}

func (r *dockerContainerRunner) RunContainer(
	ctx context.Context, spec ContainerSpec, hooks RunHooks,
) (int, error) {
	pullErr := r.pullImage(ctx, spec.Image)

	resp, err := r.cli.ContainerCreate(ctx, containerConfig(spec), containerHostConfig(spec), nil, nil, "")
	if err != nil {
		if pullErr != nil {
			return 0, fmt.Errorf("%s: %w", reasonPullFailed, pullErr)
		}

		return 0, fmt.Errorf("%s: %w", reasonCreateFailed, err)
	}

	defer r.removeContainer(resp.ID)

	if startErr := r.cli.ContainerStart(ctx, resp.ID, dockertypes.StartOptions{}); startErr != nil {
		return 0, fmt.Errorf("%s: %w", reasonStartFailed, startErr)
	}

	if hooks.OnAddress != nil && spec.Network != "" {
		ip, ipErr := r.containerIP(ctx, resp.ID)
		if ipErr != nil {
			return 0, fmt.Errorf("%s: %w", reasonStartFailed, ipErr)
		}

		hooks.OnAddress(ip)
	}

	if hooks.OnStarted != nil {
		hooks.OnStarted()
	}

	logsDone := r.streamLogs(ctx, resp.ID, hooks.OnLog)
	code, waitErr := r.awaitExit(ctx, resp.ID)

	if waitErr != nil {
		r.stopContainer(resp.ID)
	}

	<-logsDone

	return code, waitErr
}

var errNoNetworkSupport = errors.New("docker client does not support job networks")

// CreateJobNetwork creates a private bridge network; remove deletes it.
func (r *dockerContainerRunner) CreateJobNetwork(ctx context.Context, name string) (func(), error) {
	na, ok := r.cli.(networkAPI)
	if !ok {
		return nil, errNoNetworkSupport
	}

	id, err := na.NetworkCreate(ctx, name)
	if err != nil {
		return nil, err
	}

	return func() {
		rmCtx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
		defer cancel()

		_ = na.NetworkRemove(rmCtx, id)
	}, nil
}

func (r *dockerContainerRunner) containerIP(ctx context.Context, id string) (string, error) {
	na, ok := r.cli.(networkAPI)
	if !ok {
		return "", errNoNetworkSupport
	}

	return na.ContainerIP(ctx, id)
}

// pullImage is best-effort: an image present only locally still runs, and a
// missing one fails at create.
func (r *dockerContainerRunner) pullImage(ctx context.Context, image string) error {
	rc, err := r.cli.ImagePull(ctx, image, dockerimage.PullOptions{})
	if err != nil {
		return err
	}

	copyErr := docker.ConsumePullStream(rc)
	_ = rc.Close()

	return copyErr
}

func (r *dockerContainerRunner) awaitExit(ctx context.Context, id string) (int, error) {
	res := r.cli.ContainerWait(ctx, id, dockertypes.WaitOptions{})

	select {
	case w := <-res.Result:
		if w.Error != nil {
			return 0, fmt.Errorf("%w: %s", errContainerWait, w.Error.Message)
		}

		return int(w.StatusCode), nil
	case err := <-res.Error:
		return 0, fmt.Errorf("%w: %w", errContainerWait, err)
	case <-ctx.Done():
		return 0, fmt.Errorf("container cancelled: %w", ctx.Err())
	}
}

func (r *dockerContainerRunner) streamLogs(ctx context.Context, id string, onLog func([]string)) <-chan struct{} {
	done := make(chan struct{})

	if onLog == nil {
		close(done)

		return done
	}

	go func() {
		defer close(done)

		rc, err := r.cli.ContainerLogs(
			ctx,
			id,
			dockertypes.LogsOptions{ShowStdout: true, ShowStderr: true, Follow: true},
		)
		if err != nil {
			return
		}
		defer rc.Close()

		w := &lineWriter{emit: onLog}
		_, _ = stdcopy.StdCopy(w, w, rc)
		w.flush()
	}()

	return done
}

func (r *dockerContainerRunner) stopContainer(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	grace := stopGraceSeconds
	_ = r.cli.ContainerStop(ctx, id, dockertypes.StopOptions{Timeout: &grace})
}

func (r *dockerContainerRunner) removeContainer(id string) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()

	_ = r.cli.ContainerRemove(ctx, id, dockertypes.RemoveOptions{Force: true, RemoveVolumes: true})
}

var errContainerWait = errors.New("container wait failed")

// containerLabels stamps the gopherstack identity without a resource key: a job lost to a crash fails, so its
// container is removed at the next startup instead of being held for adoption.
func containerLabels(spec ContainerSpec) map[string]string {
	l := container.ManagedLabels("batch", "")
	maps.Copy(l, spec.Labels)

	return l
}

func containerConfig(spec ContainerSpec) *dockertypes.Config {
	return &dockertypes.Config{
		Image:  spec.Image,
		User:   spec.User,
		Cmd:    spec.Command,
		Env:    spec.Env,
		Labels: containerLabels(spec),
	}
}

func containerHostConfig(spec ContainerSpec) *dockertypes.HostConfig {
	hc := &dockertypes.HostConfig{Privileged: spec.Privileged, ReadonlyRootfs: spec.ReadonlyRootFS}

	if spec.Network != "" {
		hc.NetworkMode = dockertypes.NetworkMode(spec.Network)
	}

	if spec.MemoryMiB > 0 {
		hc.Memory = max(spec.MemoryMiB, dockerMinMemoryMiB) * bytesPerMiB
	}

	if spec.VCPUs > 0 {
		hc.CPUShares = int64(spec.VCPUs * cpuSharesPerVCPU)
	}

	return hc
}

// lineWriter splits a byte stream into lines and emits each Write's complete lines.
type lineWriter struct {
	emit func([]string)
	buf  []byte
	mu   sync.Mutex
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf = append(w.buf, p...)

	var lines []string

	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}

		lines = append(lines, string(bytes.TrimSuffix(w.buf[:i], []byte("\r"))))
		w.buf = w.buf[i+1:]
	}

	if len(lines) > 0 {
		w.emit(lines)
	}

	return len(p), nil
}

func (w *lineWriter) flush() {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.buf) > 0 {
		w.emit([]string{string(w.buf)})
		w.buf = nil
	}
}
