package ecs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/moby/moby/api/pkg/stdcopy"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	dockertypes "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	dockerimage "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	dockernetwork "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/network"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"

	"github.com/blackbirdworks/gopherstack/pkgs/docker"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/registryauth"
)

// dockerClient is the subset of the Docker API used by realDockerRunner.
// It is defined as an interface to allow injection of fakes in tests.
type dockerClient interface {
	ImagePull(
		ctx context.Context,
		refStr string,
		options dockerimage.PullOptions,
	) (io.ReadCloser, error)
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
	ContainerRemove(
		ctx context.Context,
		containerID string,
		options dockertypes.RemoveOptions,
	) error
	// ContainerLogs matches github.com/moby/moby/client@v0.5.1's
	// (*Client).ContainerLogs(ctx, containerID, ContainerLogsOptions)
	// (ContainerLogsResult, error), with ContainerLogsResult being exactly
	// io.ReadCloser and the compat option type substituted in, so the
	// concrete client (internal/dockercompat/client.Client) satisfies this
	// unchanged.
	ContainerLogs(
		ctx context.Context,
		containerID string,
		options dockertypes.LogsOptions,
	) (io.ReadCloser, error)
	// ContainerWait matches github.com/moby/moby/client@v0.5.1's
	// (*Client).ContainerWait(ctx, containerID, ContainerWaitOptions)
	// ContainerWaitResult, with the compat option/result types substituted in,
	// so the concrete client (internal/dockercompat/client.Client) satisfies
	// this unchanged.
	ContainerWait(
		ctx context.Context,
		containerID string,
		options dockertypes.WaitOptions,
	) dockertypes.WaitResult
	ContainerStats(ctx context.Context, containerID string) (io.ReadCloser, error)
	ContainerInspect(ctx context.Context, containerID string) (dockertypes.InspectResponse, error)
	VolumeExists(ctx context.Context, name string) (bool, error)
	VolumeRemove(ctx context.Context, name string) error
}

// NewDockerRunner creates a TaskRunner backed by the local Docker daemon.
// It uses the standard DOCKER_HOST / DOCKER_TLS_VERIFY environment variables
// via client.FromEnv, so it works both locally and inside docker-in-docker.
func NewDockerRunner(ctx context.Context) (TaskRunner, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return newDockerRunnerWithClient(ctx, cli), nil
}

// newDockerRunnerWithClient creates a realDockerRunner using the provided dockerClient.
// This constructor is used by tests to inject a fake Docker client.
func newDockerRunnerWithClient(ctx context.Context, cli dockerClient) *realDockerRunner {
	if ctx == nil {
		ctx = context.Background()
	}

	return &realDockerRunner{
		cli:               cli,
		containers:        make(map[string][]string),
		stopOrder:         make(map[string][]string),
		taskVolumes:       make(map[string][]string),
		logCancels:        make(map[string]context.CancelFunc),
		waitCancels:       make(map[string]context.CancelFunc),
		healthCancels:     make(map[string]context.CancelFunc),
		exits:             make(map[string]*containerExit),
		stopTimeouts:      make(map[string]int),
		launching:         make(map[string]*launchState),
		fronts:            newSGFronts(),
		svcCtx:            ctx,
		now:               time.Now,
		pollInterval:      defaultPollInterval,
		dependencyTimeout: defaultDependencyTimeout,
	}
}

// realDockerRunner is a TaskRunner that launches Docker containers.
type realDockerRunner struct {
	containers        map[string][]string
	stopOrder         map[string][]string
	taskVolumes       map[string][]string
	logCancels        map[string]context.CancelFunc
	waitCancels       map[string]context.CancelFunc
	healthCancels     map[string]context.CancelFunc
	exits             map[string]*containerExit
	stopTimeouts      map[string]int
	launching         map[string]*launchState
	cli               dockerClient
	claim             claimFunc
	cwLogs            CWLogsBackend
	completionHandler func(taskArn, containerName string, exitCode int)
	healthHandler     func(taskArn, containerName, status string)
	reasonHandler     func(taskArn, containerName, reason string)
	secretLookup      func(ctx context.Context, valueFrom string) (string, error)
	efsLookup         func(region string, cfg *EFSVolumeConfiguration) (EFSVolumeMount, error)
	registryAuth      func(ctx context.Context, cd ContainerDefinition) (string, error)
	envFileLookup     func(ctx context.Context, cd ContainerDefinition) ([]string, error)
	taskEnv           func(task *Task, containerName string) []string
	attach            func(ctx context.Context, containerID string) error
	now               func() time.Time
	svcCtx            context.Context
	fronts            *sgFronts
	pollInterval      time.Duration
	dependencyTimeout time.Duration
	logWG             sync.WaitGroup
	waitWG            sync.WaitGroup
	mu                sync.Mutex
}

// SetCWLogsBackend wires CloudWatch Logs so this runner forwards awslogs-driver
// containers' stdout/stderr as they run. Implements cwLogsRunner (logs.go);
// called via InMemoryBackend.SetCWLogsBackend. Safe to call concurrently
// with RunTask.
func (r *realDockerRunner) SetCWLogsBackend(cwl CWLogsBackend) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cwLogs = cwl
}

func (r *realDockerRunner) cwLogsBackend() CWLogsBackend {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.cwLogs
}

// SetTaskCompletionHandler wires fn to be called when a container this runner
// started exits on its own -- not via an explicit StopTask/rollback, which
// cancel the watch instead (see cancelContainerWait) -- so the caller can move
// the owning task to STOPPED. Safe to call concurrently with RunTask.
func (r *realDockerRunner) SetTaskCompletionHandler(fn func(taskArn, containerName string, exitCode int)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.completionHandler = fn
}

// SetTaskEnvProvider wires the ECS runtime environment injected into every container this runner creates.
func (r *realDockerRunner) SetTaskEnvProvider(fn func(task *Task, containerName string) []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.taskEnv = fn
}

// SetContainerAttacher wires the hook that runs on each created container before it starts.
func (r *realDockerRunner) SetContainerAttacher(fn func(ctx context.Context, containerID string) error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.attach = fn
}

func (r *realDockerRunner) attachContainer(ctx context.Context, containerID string) error {
	r.mu.Lock()
	fn := r.attach
	r.mu.Unlock()

	if fn == nil {
		return nil
	}

	return fn(ctx, containerID)
}

func (r *realDockerRunner) runtimeEnv(task *Task, containerName string) []string {
	r.mu.Lock()
	fn := r.taskEnv
	r.mu.Unlock()

	if fn == nil {
		return nil
	}

	return fn(task, containerName)
}

func (r *realDockerRunner) taskCompletionHandler() func(taskArn, containerName string, exitCode int) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.completionHandler
}

// rollbackContainers stops and force-removes a set of already-started
// containers.  Errors are logged but not returned so that all containers are
// attempted.  This is called on the error path of RunTask to ensure the
// task leaves no running containers behind.
func (r *realDockerRunner) rollbackContainers(ctx context.Context, containerIDs []string) {
	log := logger.Load(ctx)
	timeout := 10

	for _, id := range containerIDs {
		// These containers are being torn down unconditionally below (force
		// remove), so stop any in-flight log forwarder before that happens
		// rather than leaving it to notice via a Follow read error. Likewise
		// for the exit watcher: this is a rollback, not a natural exit, so it
		// must not report task completion.
		r.cancelLogForwarding(id)
		r.cancelContainerWait(id)
		r.closeFronts(id)

		if err := r.cli.ContainerStop(ctx, id, dockertypes.StopOptions{Timeout: &timeout}); err != nil {
			log.WarnContext(
				ctx,
				"failed to stop container during rollback",
				"containerID",
				id,
				"error",
				err,
			)
		}

		if err := r.cli.ContainerRemove(
			ctx,
			id,
			dockertypes.RemoveOptions{Force: true, RemoveVolumes: true},
		); err != nil {
			log.WarnContext(
				ctx,
				"failed to remove container during rollback",
				"containerID",
				id,
				"error",
				err,
			)
		}
	}
}

// pullImage pulls a Docker image and drains the response body.
func (r *realDockerRunner) pullImage(ctx context.Context, image, auth string) error {
	if auth != "" {
		return r.pullOnce(ctx, image, auth)
	}

	return registryauth.Pull(ctx, image, r.pullOnce, registryauth.TaggerFrom(r.cli))
}

func (r *realDockerRunner) pullOnce(ctx context.Context, image, auth string) error {
	reader, err := r.cli.ImagePull(ctx, image, dockerimage.PullOptions{RegistryAuth: auth})
	if err != nil {
		return fmt.Errorf("pull image %s: %w", image, err)
	}

	if copyErr := docker.ConsumePullStream(reader); copyErr != nil {
		_ = reader.Close()

		return fmt.Errorf("drain image pull response for %s: %w", image, copyErr)
	}

	if closeErr := reader.Close(); closeErr != nil {
		return fmt.Errorf("close image pull reader for %s: %w", image, closeErr)
	}

	return nil
}

// buildPortMappings converts PortMappings to Moby network.PortMap and network.PortSet.
func buildPortMappings(mappings []PortMapping) (dockernetwork.PortMap, dockernetwork.PortSet) {
	portBindings := dockernetwork.PortMap{}
	exposedPorts := dockernetwork.PortSet{}

	for _, pm := range mappings {
		proto := pm.Protocol
		if proto == "" {
			proto = transportTCP
		}

		if pr, isRange := parsePortRange(pm.ContainerPortRange); isRange {
			for p := pr.start; p <= pr.end; p++ {
				if rp, rerr := dockernetwork.ParsePort(fmt.Sprintf("%d/%s", p, proto)); rerr == nil {
					exposedPorts[rp] = struct{}{}
				}
			}

			continue
		}

		containerPort, err := dockernetwork.ParsePort(fmt.Sprintf("%d/%s", pm.ContainerPort, proto))
		if err != nil || pm.ContainerPort == 0 {
			continue
		}

		exposedPorts[containerPort] = struct{}{}

		if pm.HostPort > 0 {
			portBindings[containerPort] = []dockernetwork.PortBinding{
				{HostIP: netip.MustParseAddr("0.0.0.0"), HostPort: strconv.Itoa(pm.HostPort)},
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

// ContainerIDs returns the Docker container IDs started for taskArn, in task-definition container order.
func (r *realDockerRunner) ContainerIDs(taskArn string) []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]string(nil), r.containers[taskArn]...)
}

// forwardContainerLogs streams a just-started container's stdout/stderr to
// CloudWatch Logs, in its own goroutine so RunTask returns without waiting
// for the container to exit. The goroutine ends -- and releases its map
// entry and logWG slot -- when the log stream reaches EOF (container exit or
// removal, in production) or cancelLogForwarding(containerID) is called.
func (r *realDockerRunner) forwardContainerLogs(cwLogs CWLogsBackend, containerID, group, stream, since string) {
	ctx, cancel := context.WithCancel(r.svcCtx)

	r.mu.Lock()
	r.logCancels[containerID] = cancel
	r.mu.Unlock()

	r.logWG.Go(func() {
		defer cancel()
		defer func() {
			r.mu.Lock()
			delete(r.logCancels, containerID)
			r.mu.Unlock()
		}()

		rc, err := r.cli.ContainerLogs(ctx, containerID, dockertypes.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
			Since:      since,
		})
		if err != nil {
			return
		}
		defer rc.Close()

		w := &awslogsWriter{cwLogs: cwLogs, group: group, stream: stream}
		// Not using err: StdCopy's error is either a Systemerr frame or an
		// I/O error, neither actionable once we're mid-stream best-effort
		// forwarding logs; it always returns on EOF regardless.
		_, _ = stdcopy.StdCopy(w, w, rc)
		w.flush()
	})
}

// cancelLogForwarding stops the in-flight log forwarder for containerID, if
// any. Called once a container is confirmed stopped/removed so its forwarder
// goroutine doesn't block on Follow indefinitely against a dead container.
func (r *realDockerRunner) cancelLogForwarding(containerID string) {
	r.mu.Lock()
	cancel, ok := r.logCancels[containerID]
	r.mu.Unlock()

	if ok {
		cancel()
	}
}

// awslogsWriter demultiplexed via stdcopy.StdCopy writes both stdout and
// stderr to the same instance: the awslogs driver interleaves both into one
// CloudWatch Logs stream, same as the real ECS agent. It splits the combined
// bytes into lines and forwards each as it completes.
type awslogsWriter struct {
	cwLogs CWLogsBackend
	group  string
	stream string
	buf    []byte
}

func (w *awslogsWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)

	for {
		idx := bytes.IndexByte(w.buf, '\n')
		if idx < 0 {
			break
		}

		line := string(w.buf[:idx])
		w.buf = w.buf[idx+1:]
		_ = w.cwLogs.PutLogLines(w.group, w.stream, []string{line})
	}

	return len(p), nil
}

// flush forwards a final line left over with no trailing newline.
func (w *awslogsWriter) flush() {
	if len(w.buf) == 0 {
		return
	}

	_ = w.cwLogs.PutLogLines(w.group, w.stream, []string{string(w.buf)})
	w.buf = nil
}

// newTaskRunner creates the appropriate TaskRunner based on the
// GOPHERSTACK_ECS_RUNTIME environment variable.
// Returns a no-op runner when the environment variable is absent or "none".
func newTaskRunner(ctx context.Context) (TaskRunner, error) {
	switch os.Getenv("GOPHERSTACK_ECS_RUNTIME") {
	case "docker":
		return NewDockerRunner(ctx)
	default:
		// "none" or unset – no-op
		return NewNoopRunner(), nil
	}
}
