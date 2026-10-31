package container

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	dockercontainer "github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/container"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/filters"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/image"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/api/types/network"
	"github.com/blackbirdworks/gopherstack/internal/dockercompat/client"

	"github.com/blackbirdworks/gopherstack/pkgs/docker"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/registryauth"
)

// defaultIdleTimeout is the duration after which an idle container is reaped.
const defaultIdleTimeout = 10 * time.Minute

// defaultPoolSize is the default maximum warm containers per image.
const defaultPoolSize = 3

// reaperIntervalDivisor is the divisor used to compute the default reaper interval
// from the idle timeout. Reaper runs at half the idle timeout by default.
const reaperIntervalDivisor = 2

// minReaperInterval is the minimum allowed ticker interval to prevent a zero or
// negative duration being passed to [time.NewTicker] (which would panic).
const minReaperInterval = time.Millisecond

// stopTimeoutSecs is the timeout in seconds for stopping a container.
const stopTimeoutSecs = 10

// APIClient is a subset of the Docker/Podman SDK client interface used by DockerRuntime.
// It is defined as an interface to enable testing without a real container daemon.
type APIClient interface {
	ImagePull(ctx context.Context, refStr string, options image.PullOptions) (io.ReadCloser, error)
	ImageList(ctx context.Context, options image.ListOptions) ([]image.Summary, error)
	ContainerCreate(
		ctx context.Context,
		cfg *dockercontainer.Config,
		hostConfig *dockercontainer.HostConfig,
		networkingConfig any,
		platform any,
		containerName string,
	) (dockercontainer.CreateResponse, error)
	ContainerStart(ctx context.Context, containerID string, options dockercontainer.StartOptions) error
	ContainerStop(ctx context.Context, containerID string, options dockercontainer.StopOptions) error
	ContainerRemove(ctx context.Context, containerID string, options dockercontainer.RemoveOptions) error
	Ping(ctx context.Context) (any, error)
	Close() error
}

// ExecAPIClient is implemented by clients that can run commands inside a container.
type ExecAPIClient interface {
	ContainerExec(
		ctx context.Context,
		containerID string,
		cmd []string,
	) (stdout, stderr []byte, exitCode int, err error)
}

// ExecEnvAPIClient is implemented by clients that can run a command with extra environment variables.
type ExecEnvAPIClient interface {
	ContainerExecEnv(
		ctx context.Context,
		containerID string,
		cmd, env []string,
	) (stdout, stderr []byte, exitCode int, err error)
}

// ExecStreamAPIClient is implemented by clients that can stream a command's output while it runs.
type ExecStreamAPIClient interface {
	ContainerExecStream(
		ctx context.Context,
		containerID string,
		cmd []string,
		stdout, stderr io.Writer,
	) (int, error)
}

// NetworkAPIClient is implemented by clients that can manage user-defined networks.
type NetworkAPIClient interface {
	NetworkCreate(ctx context.Context, name string) (string, error)
	NetworkConnect(ctx context.Context, networkID, containerID string) error
	NetworkRemove(ctx context.Context, networkID string) error
}

// ListAPIClient is implemented by clients that can list containers by label.
type ListAPIClient interface {
	ContainerListByLabel(ctx context.Context, label string) ([]ManagedContainer, error)
}

// realDockerClient wraps the standard Docker SDK client to satisfy APIClient.
type realDockerClient struct {
	c *client.Client
}

func (r *realDockerClient) ImagePush(ctx context.Context, ref string) (string, error) {
	return r.c.ImagePush(ctx, ref)
}

func (r *realDockerClient) ImageTag(ctx context.Context, source, target string) error {
	return r.c.ImageTag(ctx, source, target)
}

func (r *realDockerClient) ImagePull(
	ctx context.Context,
	refStr string,
	options image.PullOptions,
) (io.ReadCloser, error) {
	return r.c.ImagePull(ctx, refStr, options)
}

func (r *realDockerClient) ImageList(ctx context.Context, options image.ListOptions) ([]image.Summary, error) {
	return r.c.ImageList(ctx, options)
}

func (r *realDockerClient) ContainerCreate(
	ctx context.Context,
	cfg *dockercontainer.Config,
	hostConfig *dockercontainer.HostConfig,
	_ any,
	_ any,
	containerName string,
) (dockercontainer.CreateResponse, error) {
	return r.c.ContainerCreate(ctx, cfg, hostConfig, nil, nil, containerName)
}

func (r *realDockerClient) ContainerStart(
	ctx context.Context,
	containerID string,
	options dockercontainer.StartOptions,
) error {
	return r.c.ContainerStart(ctx, containerID, options)
}

func (r *realDockerClient) ContainerStop(
	ctx context.Context,
	containerID string,
	options dockercontainer.StopOptions,
) error {
	return r.c.ContainerStop(ctx, containerID, options)
}

func (r *realDockerClient) ContainerRemove(
	ctx context.Context,
	containerID string,
	options dockercontainer.RemoveOptions,
) error {
	return r.c.ContainerRemove(ctx, containerID, options)
}

func (r *realDockerClient) ContainerExec(
	ctx context.Context,
	containerID string,
	cmd []string,
) ([]byte, []byte, int, error) {
	return r.c.ContainerExec(ctx, containerID, cmd)
}

func (r *realDockerClient) ContainerExecEnv(
	ctx context.Context,
	containerID string,
	cmd, env []string,
) ([]byte, []byte, int, error) {
	return r.c.ContainerExecEnv(ctx, containerID, cmd, env)
}

func (r *realDockerClient) ContainerExecStream(
	ctx context.Context,
	containerID string,
	cmd []string,
	stdout, stderr io.Writer,
) (int, error) {
	return r.c.ContainerExecStream(ctx, containerID, cmd, stdout, stderr)
}

func (r *realDockerClient) ContainerListByLabel(ctx context.Context, label string) ([]ManagedContainer, error) {
	items, err := r.c.ContainerList(ctx, dockercontainer.ListOptions{
		All:     true,
		Filters: filters.NewArgs(filters.KeyValuePair{Key: "label", Value: label}),
	})
	if err != nil {
		return nil, err
	}

	out := make([]ManagedContainer, 0, len(items))

	for _, it := range items {
		mc := ManagedContainer{
			ID: it.ID, Labels: it.Labels, Running: string(it.State) == "running", Ports: map[int]int{},
		}
		if len(it.Names) > 0 {
			mc.Name = strings.TrimPrefix(it.Names[0], "/")
		}

		for _, p := range it.Ports {
			if p.PublicPort != 0 {
				mc.Ports[int(p.PrivatePort)] = int(p.PublicPort)
			}
		}

		out = append(out, mc)
	}

	return out, nil
}

func (r *realDockerClient) NetworkCreate(ctx context.Context, name string) (string, error) {
	return r.c.NetworkCreate(ctx, name)
}

func (r *realDockerClient) NetworkConnect(ctx context.Context, networkID, containerID string) error {
	return r.c.NetworkConnect(ctx, networkID, containerID)
}

func (r *realDockerClient) NetworkRemove(ctx context.Context, networkID string) error {
	return r.c.NetworkRemove(ctx, networkID)
}

func (r *realDockerClient) VolumeCreate(ctx context.Context, name string, labels map[string]string) error {
	return r.c.VolumeCreate(ctx, name, labels)
}

func (r *realDockerClient) VolumeRemove(ctx context.Context, name string) error {
	return r.c.VolumeRemove(ctx, name)
}

func (r *realDockerClient) VolumeListUnused(ctx context.Context, label string) (map[string]time.Time, error) {
	return r.c.VolumeListUnused(ctx, label)
}

func (r *realDockerClient) Ping(ctx context.Context) (any, error) {
	return r.c.Ping(ctx)
}

func (r *realDockerClient) Runtimes(ctx context.Context) ([]string, error) {
	return r.c.Runtimes(ctx)
}

func (r *realDockerClient) Close() error {
	return r.c.Close()
}

// DockerRuntime implements Runtime using the Docker daemon.
type DockerRuntime struct {
	docker APIClient
	pools  map[string][]*PooledContainer
	mu     *lockmetrics.RWMutex
	cfg    Config
}

// newDockerRuntime creates a DockerRuntime connected to the Docker daemon.
// Returns ErrUnavailable if Docker is not reachable.
func newDockerRuntime(cfg Config) (*DockerRuntime, error) {
	sdkClient, err := client.NewClientWithOpts(client.FromEnv(), client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnavailable, err)
	}

	return newDockerRuntimeWithAPI(&realDockerClient{c: sdkClient}, cfg), nil
}

// NewDockerRuntimeWithAPI creates a DockerRuntime with an injected APIClient.
// This is primarily intended for testing; production code should use NewRuntime.
func NewDockerRuntimeWithAPI(api APIClient, cfg Config) *DockerRuntime {
	return newDockerRuntimeWithAPI(api, cfg)
}

// newDockerRuntimeWithAPI creates a DockerRuntime with an injected APIClient.
func newDockerRuntimeWithAPI(api APIClient, cfg Config) *DockerRuntime {
	if cfg.PoolSize <= 0 {
		cfg.PoolSize = defaultPoolSize
	}

	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = defaultIdleTimeout
	}

	return &DockerRuntime{
		pools:  make(map[string][]*PooledContainer),
		cfg:    cfg,
		docker: api,
		mu:     lockmetrics.New("container.runtime"),
	}
}

// Ping checks whether the Docker daemon is reachable.
func (r *DockerRuntime) Ping(ctx context.Context) error {
	_, err := r.docker.Ping(ctx)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrUnavailable, err)
	}

	return nil
}

// PullImage pulls the specified image from the registry.
func (r *DockerRuntime) PullImage(ctx context.Context, imageRef string) error {
	return registryauth.Pull(ctx, imageRef, r.pullOnce, registryauth.TaggerFrom(r.docker))
}

// PullImageWithCredentials pulls imageRef authenticating to the registry as username/password.
func (r *DockerRuntime) PullImageWithCredentials(ctx context.Context, imageRef, username, password string) error {
	host := registryauth.HostOf(imageRef)

	return r.pullOnce(ctx, imageRef, registryauth.EncodeCredential(host, username, password))
}

func (r *DockerRuntime) pullOnce(ctx context.Context, ref, auth string) error {
	rc, err := r.docker.ImagePull(ctx, ref, image.PullOptions{RegistryAuth: auth})
	if err != nil {
		return fmt.Errorf("image pull %q: %w", ref, err)
	}
	defer rc.Close()

	if w, ok := rc.(interface{ Wait(context.Context) error }); ok {
		if werr := w.Wait(ctx); werr != nil {
			return fmt.Errorf("image pull %q: %w", ref, werr)
		}

		return nil
	}

	if perr := docker.ConsumePullStream(rc); perr != nil {
		return fmt.Errorf("image pull %q: %w", ref, perr)
	}

	return nil
}

const (
	pullAttempts     = 3
	pullRetryBackoff = 3 * time.Second
)

// pullWithRetry retries a failed pull: registries throttle and drop streams on busy CI hosts.
func (r *DockerRuntime) pullWithRetry(ctx context.Context, imageRef string) error {
	var err error

	for attempt := range pullAttempts {
		if err = r.PullImage(ctx, imageRef); err == nil {
			return nil
		}

		if attempt == pullAttempts-1 {
			break
		}

		select {
		case <-ctx.Done():
			return err
		case <-time.After(pullRetryBackoff * time.Duration(attempt+1)):
		}
	}

	return err
}

// HasImage reports whether the given image reference is already present locally.
func (r *DockerRuntime) HasImage(ctx context.Context, imageRef string) (bool, error) {
	filter := filters.NewArgs(filters.KeyValuePair{Key: "reference", Value: imageRef})
	images, err := r.docker.ImageList(ctx, image.ListOptions{Filters: filter})
	if err != nil {
		return false, fmt.Errorf("image list: %w", err)
	}

	return len(images) > 0, nil
}

// CreateAndStart creates a new container from spec and starts it.
// Returns the container ID.
func (r *DockerRuntime) CreateAndStart(ctx context.Context, spec Spec) (string, error) {
	cfg := &dockercontainer.Config{
		Image: spec.Image,
		Env:   spec.Env,
	}

	if len(spec.Cmd) > 0 {
		cfg.Cmd = spec.Cmd
	}

	if len(spec.Entrypoint) > 0 {
		cfg.Entrypoint = spec.Entrypoint
	}

	cfg.Labels = spec.launchLabels()
	cfg.WorkingDir = spec.WorkingDir
	cfg.User = spec.User

	hostCfg := &dockercontainer.HostConfig{
		Binds:      spec.Mounts,
		Privileged: spec.Privileged,
		Runtime:    spec.OCIRuntime,
		GroupAdd:   spec.GroupAdd,
	}
	hostCfg.Memory = spec.MemoryBytes
	hostCfg.NanoCPUs = spec.NanoCPUs

	if spec.Network != "" {
		hostCfg.NetworkMode = dockercontainer.NetworkMode(spec.Network)
	}

	r.cfg.Injection.apply(cfg, hostCfg)

	if len(spec.Tmpfs) > 0 {
		hostCfg.Tmpfs = make(map[string]string, len(spec.Tmpfs))
		for _, path := range spec.Tmpfs {
			hostCfg.Tmpfs[path] = ""
		}
	}

	if err := applyPorts(cfg, hostCfg, spec.Ports); err != nil {
		return "", err
	}

	// Ensure the image is present before creating the container. Real AWS (and
	// LocalStack) pull the runtime/base image on demand; without this a clean
	// host fails container creation with "No such image".
	if has, herr := r.HasImage(ctx, spec.Image); herr == nil && !has {
		if perr := r.pullWithRetry(ctx, spec.Image); perr != nil {
			return "", fmt.Errorf("ensure image %q: %w", spec.Image, perr)
		}
	}

	resp, err := r.docker.ContainerCreate(ctx, cfg, hostCfg, nil, nil, spec.Name)
	if err != nil {
		return "", fmt.Errorf("container create %q: %w", spec.Image, err)
	}

	if startErr := r.docker.ContainerStart(ctx, resp.ID, dockercontainer.StartOptions{}); startErr != nil {
		_ = r.docker.ContainerRemove(
			context.WithoutCancel(ctx),
			resp.ID,
			dockercontainer.RemoveOptions{Force: true, RemoveVolumes: true},
		)

		return "", fmt.Errorf("container start %q: %w", resp.ID, startErr)
	}

	return resp.ID, nil
}

// applyPorts publishes each [IP:]HOST:CONTAINER TCP pair; without an IP it binds all interfaces.
func applyPorts(cfg *dockercontainer.Config, hostCfg *dockercontainer.HostConfig, ports []string) error {
	if len(ports) == 0 {
		return nil
	}

	cfg.ExposedPorts = network.PortSet{}
	hostCfg.PortBindings = network.PortMap{}

	for _, spec := range ports {
		ip, host, ctr, err := ParsePortSpec(spec)
		if err != nil {
			return err
		}

		port, err := network.ParsePort(ctr + "/tcp")
		if err != nil {
			return fmt.Errorf("%w %q: %w", ErrInvalidPort, spec, err)
		}

		cfg.ExposedPorts[port] = struct{}{}
		hostCfg.PortBindings[port] = []network.PortBinding{{HostIP: ip, HostPort: host}}
	}

	return nil
}

// StopAndRemove stops and removes a container.
func (r *DockerRuntime) StopAndRemove(ctx context.Context, containerID string) error {
	timeout := stopTimeoutSecs

	if err := r.docker.ContainerStop(ctx, containerID, dockercontainer.StopOptions{Timeout: &timeout}); err != nil {
		return fmt.Errorf("container stop %q: %w", containerID, err)
	}

	if err := r.docker.ContainerRemove(
		ctx,
		containerID,
		dockercontainer.RemoveOptions{Force: true, RemoveVolumes: true},
	); err != nil {
		return fmt.Errorf("container remove %q: %w", containerID, err)
	}

	return nil
}

// StopContainer stops a container without removing it, keeping its published ports and data.
func (r *DockerRuntime) StopContainer(ctx context.Context, containerID string) error {
	timeout := stopTimeoutSecs

	if err := r.docker.ContainerStop(ctx, containerID, dockercontainer.StopOptions{Timeout: &timeout}); err != nil {
		return fmt.Errorf("container stop %q: %w", containerID, err)
	}

	return nil
}

// StartContainer starts a previously stopped container.
func (r *DockerRuntime) StartContainer(ctx context.Context, containerID string) error {
	if err := r.docker.ContainerStart(ctx, containerID, dockercontainer.StartOptions{}); err != nil {
		return fmt.Errorf("container start %q: %w", containerID, err)
	}

	return nil
}

// CreateNetwork creates a bridge network and returns its ID.
func (r *DockerRuntime) CreateNetwork(ctx context.Context, name string) (string, error) {
	api, ok := r.docker.(NetworkAPIClient)
	if !ok {
		return "", ErrNetworkUnsupported
	}

	return api.NetworkCreate(ctx, name)
}

// ConnectNetwork attaches a running container to the network.
func (r *DockerRuntime) ConnectNetwork(ctx context.Context, networkID, containerID string) error {
	api, ok := r.docker.(NetworkAPIClient)
	if !ok {
		return ErrNetworkUnsupported
	}

	return api.NetworkConnect(ctx, networkID, containerID)
}

// RemoveNetwork deletes the network.
func (r *DockerRuntime) RemoveNetwork(ctx context.Context, networkID string) error {
	api, ok := r.docker.(NetworkAPIClient)
	if !ok {
		return ErrNetworkUnsupported
	}

	return api.NetworkRemove(ctx, networkID)
}

// VolumeAPIClient is implemented by clients that can manage named volumes.
type VolumeAPIClient interface {
	VolumeCreate(ctx context.Context, name string, labels map[string]string) error
	VolumeRemove(ctx context.Context, name string) error
	VolumeListUnused(ctx context.Context, label string) (map[string]time.Time, error)
}

// CreateVolume creates a labelled named volume; an existing one is reused.
func (r *DockerRuntime) CreateVolume(ctx context.Context, name string, labels map[string]string) error {
	api, ok := r.docker.(VolumeAPIClient)
	if !ok {
		return ErrVolumeUnsupported
	}

	return api.VolumeCreate(ctx, name, labels)
}

// RemoveVolume force-removes a named volume; a missing volume is not an error.
func (r *DockerRuntime) RemoveVolume(ctx context.Context, name string) error {
	api, ok := r.docker.(VolumeAPIClient)
	if !ok {
		return ErrVolumeUnsupported
	}

	return api.VolumeRemove(ctx, name)
}

// ListUnusedVolumes lists the volumes carrying label that no container references and that are older than
// olderThan, so a volume a concurrent launch has just created is left alone.
func (r *DockerRuntime) ListUnusedVolumes(
	ctx context.Context,
	label string,
	olderThan time.Duration,
) ([]string, error) {
	api, ok := r.docker.(VolumeAPIClient)
	if !ok {
		return nil, ErrVolumeUnsupported
	}

	all, err := api.VolumeListUnused(ctx, label)
	if err != nil {
		return nil, err
	}

	cutoff := time.Now().Add(-olderThan)
	out := make([]string, 0, len(all))

	for name, created := range all {
		if created.Before(cutoff) {
			out = append(out, name)
		}
	}

	slices.Sort(out)

	return out, nil
}

// Exec runs cmd inside a running container and waits for it to finish.
func (r *DockerRuntime) Exec(ctx context.Context, containerID string, cmd []string) (ExecResult, error) {
	api, ok := r.docker.(ExecAPIClient)
	if !ok {
		return ExecResult{}, ErrExecUnsupported
	}

	stdout, stderr, code, err := api.ContainerExec(ctx, containerID, cmd)
	if err != nil {
		return ExecResult{}, fmt.Errorf("container exec %q: %w", containerID, err)
	}

	return ExecResult{Stdout: string(stdout), Stderr: string(stderr), ExitCode: code}, nil
}

// ExecEnv runs cmd like Exec with env (KEY=VALUE entries) set on the process instead of in its argv.
func (r *DockerRuntime) ExecEnv(ctx context.Context, containerID string, cmd, env []string) (ExecResult, error) {
	api, ok := r.docker.(ExecEnvAPIClient)
	if !ok {
		return ExecResult{}, ErrExecUnsupported
	}

	stdout, stderr, code, err := api.ContainerExecEnv(ctx, containerID, cmd, env)
	if err != nil {
		return ExecResult{}, fmt.Errorf("container exec %q: %w", containerID, err)
	}

	return ExecResult{Stdout: string(stdout), Stderr: string(stderr), ExitCode: code}, nil
}

// ExecStream runs cmd like Exec but writes stdout and stderr to out as the command produces them.
func (r *DockerRuntime) ExecStream(ctx context.Context, containerID string, cmd []string, out io.Writer) (int, error) {
	api, ok := r.docker.(ExecStreamAPIClient)
	if !ok {
		return 0, ErrExecUnsupported
	}

	code, err := api.ContainerExecStream(ctx, containerID, cmd, out, out)
	if err != nil {
		return 0, fmt.Errorf("container exec %q: %w", containerID, err)
	}

	return code, nil
}

// AcquireWarm returns a warm container from the pool for the given image.
// If no warm container is available, a new one is created (up to PoolSize).
// The caller must call ReleaseContainer when done.
//
// Note: PoolSize is a soft limit. When multiple goroutines call AcquireWarm concurrently,
// the pool may temporarily exceed PoolSize by the number of concurrent callers that see no
// idle container and concurrently create new ones.
func (r *DockerRuntime) AcquireWarm(ctx context.Context, spec Spec) (*PooledContainer, error) {
	r.mu.Lock("AcquireWarm")

	pool := r.pools[spec.Image]
	for _, pc := range pool {
		if !pc.InUse {
			pc.InUse = true
			pc.LastUsed = time.Now()
			r.mu.Unlock()

			return pc, nil
		}
	}

	if len(pool) >= r.cfg.PoolSize {
		r.mu.Unlock()

		return nil, ErrPoolExhausted
	}

	r.mu.Unlock()

	id, err := r.CreateAndStart(ctx, spec)
	if err != nil {
		return nil, err
	}

	pc := &PooledContainer{
		ID:       id,
		Image:    spec.Image,
		LastUsed: time.Now(),
		InUse:    true,
	}

	r.mu.Lock("AcquireWarm")
	r.pools[spec.Image] = append(r.pools[spec.Image], pc)
	r.mu.Unlock()

	return pc, nil
}

// ReleaseContainer marks a pooled container as idle.
// If the container is not in the pool, ErrContainerNotFound is returned.
func (r *DockerRuntime) ReleaseContainer(containerID string) error {
	r.mu.Lock("ReleaseContainer")
	defer r.mu.Unlock()

	for _, pool := range r.pools {
		for _, pc := range pool {
			if pc.ID == containerID {
				pc.InUse = false
				pc.LastUsed = time.Now()

				return nil
			}
		}
	}

	return fmt.Errorf("%w: %s", ErrContainerNotFound, containerID)
}

// ReapIdleContainers stops and removes containers that have been idle longer than IdleTimeout.
func (r *DockerRuntime) ReapIdleContainers(ctx context.Context) {
	r.mu.Lock("ReapIdleContainers")
	toReap := r.collectIdleLocked()
	r.mu.Unlock()

	for _, entry := range toReap {
		if err := r.StopAndRemove(ctx, entry.ID); err != nil {
			if r.cfg.Logger != nil {
				r.cfg.Logger.WarnContext(ctx, "container: failed to reap idle container",
					"id", entry.ID, "image", entry.Image, "error", err)
			}
		} else {
			if r.cfg.Logger != nil {
				r.cfg.Logger.DebugContext(ctx, "container: reaped idle container", "id", entry.ID, "image", entry.Image)
			}
		}
	}
}

// collectIdleLocked identifies idle containers to reap and removes them from the pools map.
// Must be called with r.mu held.
func (r *DockerRuntime) collectIdleLocked() []*PooledContainer {
	deadline := time.Now().Add(-r.cfg.IdleTimeout)
	var toReap []*PooledContainer

	for img, pool := range r.pools {
		var keep []*PooledContainer

		for _, pc := range pool {
			if !pc.InUse && pc.LastUsed.Before(deadline) {
				toReap = append(toReap, pc)
			} else {
				keep = append(keep, pc)
			}
		}

		r.pools[img] = keep
	}

	return toReap
}

// StartReaper launches a background goroutine that periodically reaps idle containers.
// It stops when ctx is cancelled.
func (r *DockerRuntime) StartReaper(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = r.cfg.IdleTimeout / reaperIntervalDivisor
	}

	if interval < minReaperInterval {
		interval = minReaperInterval
	}

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				r.ReapIdleContainers(ctx)
			}
		}
	}()
}

// Close releases resources held by the Docker client.
func (r *DockerRuntime) Close() error {
	return r.docker.Close()
}
