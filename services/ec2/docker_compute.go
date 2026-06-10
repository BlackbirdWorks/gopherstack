package ec2

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// defaultDockerImage is the Docker image used by the EC2 docker provider when
// none is configured; mirrors the Amazon Linux 2 AMI userland.
const defaultDockerImage = "amazonlinux:2"

// defaultSSHHostIP is the default host bind address for mapped sshd ports.
// Set to localhost so the container is only reachable from the local machine
// unless the operator explicitly opts in to 0.0.0.0.
const defaultSSHHostIP = "127.0.0.1"

// regionUSEast1 is the canonical AWS region whose public DNS zone collapses
// to "compute-1.amazonaws.com" instead of "{region}.compute.amazonaws.com".
const regionUSEast1 = "us-east-1"

// DockerComputeConfig configures a DockerCompute provider.
type DockerComputeConfig struct {
	Labels    map[string]string `json:"labels,omitempty"`
	Image     string            `json:"image,omitempty"`
	Network   string            `json:"network,omitempty"`
	SSHHostIP string            `json:"sshHostIP,omitempty"`
	// Region is used to render the public DNS name suffix
	// (e.g. us-east-1 → "compute-1.amazonaws.com").
	Region     string `json:"region,omitempty"`
	SSHPortMin int    `json:"sshPortMin,omitempty"`
	SSHPortMax int    `json:"sshPortMax,omitempty"`
}

// dockerAPI is the subset of the Docker SDK the DockerCompute uses.
// It is an interface so tests can inject a fake.
type dockerAPI interface {
	ImagePull(ctx context.Context, ref string, opts mobyclient.ImagePullOptions) (mobyclient.ImagePullResponse, error)
	ContainerCreate(
		ctx context.Context,
		options mobyclient.ContainerCreateOptions,
	) (mobyclient.ContainerCreateResult, error)
	ContainerStart(
		ctx context.Context, id string,
		opts mobyclient.ContainerStartOptions,
	) (mobyclient.ContainerStartResult, error)
	ContainerStop(
		ctx context.Context, id string,
		opts mobyclient.ContainerStopOptions,
	) (mobyclient.ContainerStopResult, error)
	ContainerRemove(
		ctx context.Context, id string,
		opts mobyclient.ContainerRemoveOptions,
	) (mobyclient.ContainerRemoveResult, error)
	ContainerInspect(
		ctx context.Context, id string,
		opts mobyclient.ContainerInspectOptions,
	) (mobyclient.ContainerInspectResult, error)
	Ping(ctx context.Context, opts mobyclient.PingOptions) (mobyclient.PingResult, error)
	Close() error
}

// DockerCompute backs EC2 instances with real Docker containers running sshd.
type DockerCompute struct {
	api          dockerAPI
	usedSSHPorts map[int]struct{}
	cfg          DockerComputeConfig
	mu           sync.Mutex
}

// NewDockerCompute creates a Compute provider backed by the local Docker
// daemon. Honours DOCKER_HOST / DOCKER_TLS_VERIFY via mobyclient.FromEnv.
func NewDockerCompute(cfg DockerComputeConfig) (*DockerCompute, error) {
	cli, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("create docker client: %w", err)
	}

	return newDockerComputeWithAPI(cli, cfg), nil
}

// newDockerComputeWithAPI is exposed for tests with a fake dockerAPI.
func newDockerComputeWithAPI(api dockerAPI, cfg DockerComputeConfig) *DockerCompute {
	if cfg.Image == "" {
		cfg.Image = defaultDockerImage
	}

	if cfg.SSHHostIP == "" {
		cfg.SSHHostIP = defaultSSHHostIP
	}

	return &DockerCompute{api: api, cfg: cfg, usedSSHPorts: make(map[int]struct{})}
}

// Ping verifies the Docker daemon is reachable.
func (d *DockerCompute) Ping(ctx context.Context) error {
	if _, err := d.api.Ping(ctx, mobyclient.PingOptions{}); err != nil {
		return fmt.Errorf("docker ping: %w", err)
	}

	return nil
}

// Close releases the underlying Docker client.
func (d *DockerCompute) Close() error {
	if err := d.api.Close(); err != nil {
		return fmt.Errorf("docker close: %w", err)
	}

	return nil
}

const (
	dockerLabelPrefix = "gopherstack.ec2."
	stopTimeoutSec    = 5
	inspectMaxRetries = 60
	inspectRetryDelay = 250 * time.Millisecond
)

// Sentinel errors for the docker-compute provider. Wrapped (via %w) by callers
// where additional context is helpful.
var (
	errInspectTimeout    = errors.New("docker compute: container did not get an IP in time")
	errSSHPortRangeEmpty = errors.New("docker compute: no free ssh host port in configured range")
)

// containerSSHPort is the in-container TCP port sshd listens on.
//
// constructed via ParsePort, so a package-level cached value is the cleanest
// option.
//
//nolint:gochecknoglobals // mobynetwork.Port has unexported fields and must be
var containerSSHPort = mustParsePort("22/tcp")

func mustParsePort(s string) mobynetwork.Port {
	p, err := mobynetwork.ParsePort(s)
	if err != nil {
		panic(err)
	}

	return p
}

// Launch implements Compute.
func (d *DockerCompute) Launch(ctx context.Context, req LaunchRequest) (LaunchResult, error) {
	if err := d.ensureImage(ctx); err != nil {
		return LaunchResult{}, err
	}

	hostPort, err := d.reserveSSHPort()
	if err != nil {
		return LaunchResult{}, err
	}

	entrypoint := buildEntrypointScript(req.AuthorizedKey, req.UserData)
	hostCfg, networkCfg := d.buildContainerConfigs(hostPort)
	createOpts := mobyclient.ContainerCreateOptions{
		Config: &mobycontainer.Config{
			Image:        d.cfg.Image,
			ExposedPorts: mobynetwork.PortSet{containerSSHPort: struct{}{}},
			Cmd:          []string{"/bin/sh", "-c", entrypoint},
			Labels:       d.buildLabels(req),
			Hostname:     dockerHostname(req.InstanceID),
		},
		HostConfig:       hostCfg,
		NetworkingConfig: networkCfg,
		Name:             "gopherstack-" + req.InstanceID,
	}

	resp, createErr := d.api.ContainerCreate(ctx, createOpts)
	if createErr != nil {
		d.releaseSSHPort(hostPort)

		return LaunchResult{}, fmt.Errorf("container create: %w", createErr)
	}

	if _, startErr := d.api.ContainerStart(ctx, resp.ID, mobyclient.ContainerStartOptions{}); startErr != nil {
		d.cleanupAfterFailure(ctx, resp.ID, hostPort)

		return LaunchResult{}, fmt.Errorf("container start: %w", startErr)
	}

	privateIP, mappedPort, inspectErr := d.waitForRunning(ctx, resp.ID)
	if inspectErr != nil {
		d.cleanupAfterFailure(ctx, resp.ID, hostPort)

		return LaunchResult{}, inspectErr
	}

	resolvedPort := hostPort
	if mappedPort != 0 {
		resolvedPort = mappedPort
	}

	return LaunchResult{
		ProviderID: resp.ID,
		PrivateIP:  privateIP,
		// PublicIPAddress is set to the host SSHHostIP so the demo can ssh
		// into the container via the published port.
		PublicIPAddress: d.cfg.SSHHostIP,
		PublicDNSName:   syntheticPublicDNS(req.InstanceID, d.cfg.Region),
		SSHPort:         resolvedPort,
	}, nil
}

// syntheticPublicDNS renders an AWS-style public hostname for an instance,
// uniquely keyed by the instance ID so the embedded DNS server can register
// one entry per launched container. The form mirrors what real EC2 returns:
// us-east-1 collapses to "compute-1", all other regions get "{region}.compute".
func syntheticPublicDNS(instanceID, region string) string {
	suffix := strings.TrimPrefix(instanceID, "i-")
	if suffix == "" {
		suffix = instanceID
	}

	zone := "compute-1.amazonaws.com"
	if region != "" && region != regionUSEast1 {
		zone = region + ".compute.amazonaws.com"
	}

	return fmt.Sprintf("ec2-%s.%s", suffix, zone)
}

// Terminate implements Compute.
func (d *DockerCompute) Terminate(ctx context.Context, instanceID, providerID string) error {
	if providerID == "" {
		return nil
	}

	log := logger.Load(ctx)
	timeout := stopTimeoutSec

	if _, err := d.api.ContainerStop(ctx, providerID, mobyclient.ContainerStopOptions{Timeout: &timeout}); err != nil {
		log.WarnContext(ctx, "docker stop on terminate", "instance", instanceID, "container", providerID, "error", err)
	}

	if _, err := d.api.ContainerRemove(
		ctx, providerID,
		mobyclient.ContainerRemoveOptions{Force: true},
	); err != nil {
		return fmt.Errorf("container remove: %w", err)
	}

	d.releaseProviderPort(ctx, providerID)

	return nil
}

// Start implements Compute.
func (d *DockerCompute) Start(ctx context.Context, _, providerID string) error {
	if providerID == "" {
		return nil
	}

	if _, err := d.api.ContainerStart(ctx, providerID, mobyclient.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("container start: %w", err)
	}

	return nil
}

// Stop implements Compute.
func (d *DockerCompute) Stop(ctx context.Context, _, providerID string) error {
	if providerID == "" {
		return nil
	}

	timeout := stopTimeoutSec
	if _, err := d.api.ContainerStop(
		ctx, providerID,
		mobyclient.ContainerStopOptions{Timeout: &timeout},
	); err != nil {
		return fmt.Errorf("container stop: %w", err)
	}

	return nil
}

// ensureImage pulls the configured image when not present locally.
// Errors here are non-fatal — Docker create will surface "no such image"
// itself if the pull failed for the wrong reason.
func (d *DockerCompute) ensureImage(ctx context.Context) error {
	rc, err := d.api.ImagePull(ctx, d.cfg.Image, mobyclient.ImagePullOptions{})
	if err != nil {
		return fmt.Errorf("image pull %q: %w", d.cfg.Image, err)
	}

	defer func() { _ = rc.Close() }()

	if _, copyErr := io.Copy(io.Discard, rc); copyErr != nil {
		return fmt.Errorf("drain image pull: %w", copyErr)
	}

	return nil
}

func (d *DockerCompute) buildContainerConfigs(hostPort int) (*mobycontainer.HostConfig, *mobynetwork.NetworkingConfig) {
	portBindings := mobynetwork.PortMap{}

	if hostPort > 0 {
		portBindings[containerSSHPort] = []mobynetwork.PortBinding{
			{HostIP: parseHostIP(d.cfg.SSHHostIP), HostPort: strconv.Itoa(hostPort)},
		}
	}

	hostCfg := &mobycontainer.HostConfig{
		PortBindings:  portBindings,
		AutoRemove:    false,
		RestartPolicy: mobycontainer.RestartPolicy{Name: "no"},
	}

	if d.cfg.Network != "" {
		hostCfg.NetworkMode = mobycontainer.NetworkMode(d.cfg.Network)
	}

	var networkCfg *mobynetwork.NetworkingConfig
	if d.cfg.Network != "" {
		networkCfg = &mobynetwork.NetworkingConfig{
			EndpointsConfig: map[string]*mobynetwork.EndpointSettings{
				d.cfg.Network: {},
			},
		}
	}

	return hostCfg, networkCfg
}

func (d *DockerCompute) buildLabels(req LaunchRequest) map[string]string {
	labels := map[string]string{
		dockerLabelPrefix + "instance":      req.InstanceID,
		dockerLabelPrefix + "image":         req.ImageID,
		dockerLabelPrefix + "instance-type": req.InstanceType,
	}
	if req.KeyName != "" {
		labels[dockerLabelPrefix+"key-name"] = req.KeyName
	}

	maps.Copy(labels, d.cfg.Labels)

	return labels
}

// waitForRunning polls ContainerInspect until the container is running and an
// IP address is assigned, returning the discovered private IP and the host
// port mapped to sshd.
func (d *DockerCompute) waitForRunning(ctx context.Context, containerID string) (string, int, error) {
	timer := time.NewTimer(inspectRetryDelay)
	defer timer.Stop()

	for range inspectMaxRetries {
		res, err := d.api.ContainerInspect(ctx, containerID, mobyclient.ContainerInspectOptions{})
		if err != nil {
			return "", 0, fmt.Errorf("container inspect: %w", err)
		}

		ip, port := extractInspectAddrs(res.Container, d.cfg.Network)
		if ip != "" {
			return ip, port, nil
		}

		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(inspectRetryDelay)

		select {
		case <-ctx.Done():
			return "", 0, fmt.Errorf("container inspect: %w", ctx.Err())
		case <-timer.C:
		}
	}

	return "", 0, errInspectTimeout
}

// extractInspectAddrs pulls the relevant network info from an inspect result.
func extractInspectAddrs(c mobycontainer.InspectResponse, preferredNetwork string) (string, int) {
	if c.NetworkSettings == nil {
		return "", 0
	}

	return pickNetworkIP(c.NetworkSettings.Networks, preferredNetwork),
		pickHostPort(c.NetworkSettings.Ports)
}

func pickNetworkIP(networks map[string]*mobynetwork.EndpointSettings, preferred string) string {
	if preferred != "" {
		if ep, ok := networks[preferred]; ok && ep != nil && ep.IPAddress.IsValid() {
			return ep.IPAddress.String()
		}
	}

	for _, ep := range networks {
		if ep != nil && ep.IPAddress.IsValid() {
			return ep.IPAddress.String()
		}
	}

	return ""
}

func pickHostPort(ports mobynetwork.PortMap) int {
	bindings, ok := ports[containerSSHPort]
	if !ok {
		return 0
	}

	for _, b := range bindings {
		if b.HostPort == "" {
			continue
		}

		if parsed, perr := strconv.Atoi(b.HostPort); perr == nil {
			return parsed
		}
	}

	return 0
}

func (d *DockerCompute) cleanupAfterFailure(ctx context.Context, containerID string, port int) {
	timeout := stopTimeoutSec
	_, _ = d.api.ContainerStop(ctx, containerID, mobyclient.ContainerStopOptions{Timeout: &timeout})
	_, _ = d.api.ContainerRemove(ctx, containerID, mobyclient.ContainerRemoveOptions{Force: true})
	d.releaseSSHPort(port)
}

// reserveSSHPort picks the next free host port in [SSHPortMin, SSHPortMax],
// or 0 when the configured range is empty (Docker assigns an ephemeral one).
func (d *DockerCompute) reserveSSHPort() (int, error) {
	if d.cfg.SSHPortMin <= 0 || d.cfg.SSHPortMax <= 0 || d.cfg.SSHPortMax < d.cfg.SSHPortMin {
		return 0, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	for p := d.cfg.SSHPortMin; p <= d.cfg.SSHPortMax; p++ {
		if _, used := d.usedSSHPorts[p]; !used {
			d.usedSSHPorts[p] = struct{}{}

			return p, nil
		}
	}

	return 0, errSSHPortRangeEmpty
}

func (d *DockerCompute) releaseSSHPort(port int) {
	if port == 0 {
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.usedSSHPorts, port)
}

// releaseProviderPort frees the host port previously mapped to the container,
// if any, by re-inspecting the container's NetworkSettings before removal.
func (d *DockerCompute) releaseProviderPort(ctx context.Context, containerID string) {
	res, err := d.api.ContainerInspect(ctx, containerID, mobyclient.ContainerInspectOptions{})
	if err != nil {
		return
	}

	_, port := extractInspectAddrs(res.Container, d.cfg.Network)
	d.releaseSSHPort(port)
}

func parseHostIP(ip string) netip.Addr {
	if ip == "" {
		return netip.MustParseAddr("0.0.0.0")
	}

	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return netip.MustParseAddr("0.0.0.0")
	}

	return addr
}
