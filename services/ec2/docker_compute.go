package ec2

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobymount "github.com/moby/moby/api/types/mount"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"

	"github.com/blackbirdworks/gopherstack/internal/linklocal"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/docker"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/registryauth"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
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
	// Flags is a restricted docker-run style option string (see parseDockerFlags) applied to every container.
	Flags string `json:"flags,omitempty"`
	// Region is used to render the public DNS name suffix
	// (e.g. us-east-1 → "compute-1.amazonaws.com").
	Region     string `json:"region,omitempty"`
	SSHPortMin int    `json:"sshPortMin,omitempty"`
	SSHPortMax int    `json:"sshPortMax,omitempty"`
	// EBSVolumes maps EBS volumes to Docker named volumes mounted under /mnt/ebs.
	EBSVolumes bool `json:"ebsVolumes,omitempty"`
}

var (
	errNoImageTag    = errors.New("docker client cannot tag images")
	errNoImageCommit = errors.New("docker client cannot commit images")
	errLocalImage    = errors.New("docker compute: AMI is bound to a local docker image that does not exist")
)

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
	ContainerStats(
		ctx context.Context, id string,
		opts mobyclient.ContainerStatsOptions,
	) (mobyclient.ContainerStatsResult, error)
	Ping(ctx context.Context, opts mobyclient.PingOptions) (mobyclient.PingResult, error)
	Close() error
}

// DockerCompute backs EC2 instances with real Docker containers running sshd.
type DockerCompute struct {
	api   dockerAPI
	ports *sshPortPool
	nets  *netRegistry
	ll    *linkLocalState
	// fronts holds the security-group relays of enforced instances; shared across region views.
	fronts *frontRegistry
	// dialer reaches containers' loopback ports from the relays; nil means a plain net.Dialer.
	dialer sgfilter.Dialer
	// mounts tracks the EBS volumes each container holds; shared across region views.
	mounts *mountRegistry
	// claim hands out surviving containers of an earlier run; nil means container.ClaimOrphan.
	claim func(service, resource string) (container.ManagedContainer, bool)
	cfg   DockerComputeConfig
	flags dockerFlags
}

// sshPortPool is shared by every region's view of one Docker daemon so two
// regions never hand out the same host SSH port.
type sshPortPool struct {
	used map[int]struct{}
	mu   sync.Mutex
}

// NewDockerCompute creates a Compute provider backed by the local Docker
// daemon. Honours DOCKER_HOST / DOCKER_TLS_VERIFY via mobyclient.FromEnv.
func NewDockerCompute(cfg DockerComputeConfig) (*DockerCompute, error) {
	if _, err := parseDockerFlags(cfg.Flags); err != nil {
		return nil, err
	}

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

	flags, _ := parseDockerFlags(cfg.Flags)
	if flags.network != "" && cfg.Network == "" {
		cfg.Network = flags.network
	}

	return &DockerCompute{
		api:    api,
		cfg:    cfg,
		flags:  flags,
		mounts: newMountRegistry(),
		ports:  &sshPortPool{used: make(map[int]struct{})},
		nets:   newNetRegistry(),
		ll:     &linkLocalState{},

		fronts: newFrontRegistry(),
	}
}

// Ping verifies the Docker daemon is reachable.
func (d *DockerCompute) Ping(ctx context.Context) error {
	if _, err := d.api.Ping(ctx, mobyclient.PingOptions{}); err != nil {
		return fmt.Errorf("docker ping: %w", err)
	}

	return nil
}

func (d *DockerCompute) forRegion(region string) Compute {
	cp := *d
	cp.cfg.Region = region

	return &cp
}

// Close releases the underlying Docker client.
func (d *DockerCompute) Close() error {
	for _, f := range d.fronts.drain() {
		f.close()
	}

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
	errNetworkInUse      = errors.New("docker compute: vpc network still in use")
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
	image, mounts, prepErr := d.prepareLaunch(ctx, req)
	if prepErr != nil {
		return LaunchResult{}, prepErr
	}

	hostPort, err := d.reserveSSHPort()
	if err != nil {
		return LaunchResult{}, err
	}

	vn, hasNet, netErr := d.vpcNetworkFor(ctx, req.VPCID, req.VPCCIDR)
	if netErr != nil {
		d.releaseSSHPort(hostPort)

		return LaunchResult{}, netErr
	}

	ll := d.activeLinkLocal(ctx)
	if ll != nil {
		req.MetadataEndpoint = ""
	}

	createOpts := d.createOptions(req, image, mounts, hostPort, vn, hasNet)

	resp, createErr := d.api.ContainerCreate(ctx, createOpts)
	if createErr != nil {
		d.releaseSSHPort(hostPort)

		return LaunchResult{}, fmt.Errorf("container create: %w", createErr)
	}

	d.recordLaunchMounts(resp.ID, req)

	if ll != nil {
		if attachErr := ll.Attach(ctx, resp.ID, req.InstanceID); attachErr != nil {
			d.cleanupAfterFailure(ctx, resp.ID, hostPort)

			return LaunchResult{}, fmt.Errorf("link-local attach: %w", attachErr)
		}
	}

	if _, startErr := d.api.ContainerStart(ctx, resp.ID, mobyclient.ContainerStartOptions{}); startErr != nil {
		d.cleanupAfterFailure(ctx, resp.ID, hostPort)

		return LaunchResult{}, fmt.Errorf("container start: %w", startErr)
	}

	preferred := d.cfg.Network
	if hasNet {
		preferred = vn.name
	}

	got, inspectErr := d.waitForRunning(ctx, resp.ID, preferred)
	if inspectErr != nil {
		d.cleanupAfterFailure(ctx, resp.ID, hostPort)

		return LaunchResult{}, inspectErr
	}

	resolvedPort := hostPort
	if got.sshPort != 0 {
		resolvedPort = got.sshPort
	}

	if req.Admit != nil {
		f, frontErr := d.frontLaunch(ctx, resp.ID, req, hostPort, got)
		if frontErr != nil {
			d.cleanupAfterFailure(ctx, resp.ID, hostPort)

			return LaunchResult{}, frontErr
		}

		got.published, resolvedPort = f.ports, hostPort
		if f.sshPort != 0 {
			resolvedPort = f.sshPort
		}
	}

	return LaunchResult{
		ProviderID: resp.ID,
		PrivateIP:  got.ip,
		// PublicIPAddress is set to the host SSHHostIP so the demo can ssh
		// into the container via the published port.
		PublicIPAddress: d.cfg.SSHHostIP,
		PublicDNSName:   syntheticPublicDNS(req.InstanceID, d.cfg.Region),
		SSHPort:         resolvedPort,
		NetworkNote:     vn.note,
		PublishedPorts:  got.published,
	}, nil
}

// createOptions assembles the container definition for an instance.
func (d *DockerCompute) createOptions(
	req LaunchRequest, image string, mounts []mobymount.Mount, hostPort int, vn vpcNetwork, hasNet bool,
) mobyclient.ContainerCreateOptions {
	hostCfg, networkCfg, exposed := d.buildContainerConfigs(hostPort, req, vn, hasNet)
	hostCfg.Mounts = append(hostCfg.Mounts, mounts...)

	return mobyclient.ContainerCreateOptions{
		Config: &mobycontainer.Config{
			Image:        image,
			ExposedPorts: exposed,
			Cmd:          []string{"/bin/sh", "-c", buildEntrypointScript(req.AuthorizedKey, req.UserData)},
			Labels:       d.buildLabels(req),
			Env:          d.flags.apply(hostCfg, containerEnv(req), exposed),
			Hostname:     dockerHostname(req.InstanceID),
		},
		HostConfig:       hostCfg,
		NetworkingConfig: networkCfg,
		Name:             "gopherstack-" + req.InstanceID,
	}
}

// prepareLaunch resolves the image the instance runs, makes it available locally and creates its EBS volumes.
func (d *DockerCompute) prepareLaunch(ctx context.Context, req LaunchRequest) (string, []mobymount.Mount, error) {
	image := d.cfg.Image
	if req.DockerImage != "" {
		image = req.DockerImage
	}

	if err := d.ensureImage(ctx, image); err != nil {
		return "", nil, err
	}

	mounts, err := d.launchMounts(ctx, req)
	if err != nil {
		return "", nil, err
	}

	return image, mounts, nil
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
	frontPort, fronted := d.closeFronts(providerID)

	d.syncBackAll(ctx, providerID)

	if _, err := d.api.ContainerStop(ctx, providerID, mobyclient.ContainerStopOptions{Timeout: &timeout}); err != nil {
		log.WarnContext(ctx, "docker stop on terminate", "instance", instanceID, "container", providerID, "error", err)
	}

	if _, err := d.api.ContainerRemove(
		ctx, providerID,
		mobyclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true},
	); err != nil {
		return fmt.Errorf("container remove: %w", err)
	}

	if fronted {
		d.releaseSSHPort(frontPort)
	} else {
		d.releaseProviderPort(ctx, providerID)
	}

	d.releaseLinkLocal(providerID)

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

// ensureImage pulls image when not present locally. Images in the gopherstack-ec2/ namespace are
// local-only and are checked, never pulled.
func (d *DockerCompute) ensureImage(ctx context.Context, image string) error {
	if strings.HasPrefix(image, localImageRepo) {
		ia, ok := d.api.(imageAPI)
		if !ok {
			return nil
		}

		if _, err := ia.ImageInspect(ctx, image); err != nil {
			return fmt.Errorf("%w: %s: %w", errLocalImage, image, err)
		}

		return nil
	}

	return registryauth.Pull(ctx, image, d.pullOnce, registryauth.TaggerFrom(imageTagAdapter{d.api}))
}

// imageTagAdapter exposes the moby ImageTag call, when the client has one, as a registryauth.ImageTagger.
type imageTagAdapter struct{ api dockerAPI }

func (a imageTagAdapter) ImageTag(ctx context.Context, source, target string) error {
	t, ok := a.api.(interface {
		ImageTag(ctx context.Context, opts mobyclient.ImageTagOptions) (mobyclient.ImageTagResult, error)
	})
	if !ok {
		return errNoImageTag
	}

	_, err := t.ImageTag(ctx, mobyclient.ImageTagOptions{Source: source, Target: target})

	return err
}

func (d *DockerCompute) pullOnce(ctx context.Context, ref, auth string) error {
	rc, err := d.api.ImagePull(ctx, ref, mobyclient.ImagePullOptions{RegistryAuth: auth})
	if err != nil {
		return fmt.Errorf("image pull %q: %w", ref, err)
	}

	defer func() { _ = rc.Close() }()

	if copyErr := docker.ConsumePullStream(rc); copyErr != nil {
		return fmt.Errorf("drain image pull: %w", copyErr)
	}

	return nil
}

// sshBinding publishes sshd on the reserved host port, or on a loopback-only port when a relay fronts it.
func (d *DockerCompute) sshBinding(hostPort int, fronted bool) []mobynetwork.PortBinding {
	if fronted {
		return []mobynetwork.PortBinding{{HostIP: parseHostIP(publishHostIP)}}
	}

	return []mobynetwork.PortBinding{{HostIP: parseHostIP(d.cfg.SSHHostIP), HostPort: strconv.Itoa(hostPort)}}
}

func (d *DockerCompute) buildContainerConfigs(
	hostPort int, req LaunchRequest, vn vpcNetwork, hasNet bool,
) (*mobycontainer.HostConfig, *mobynetwork.NetworkingConfig, mobynetwork.PortSet) {
	portBindings := mobynetwork.PortMap{}
	exposed := mobynetwork.PortSet{containerSSHPort: struct{}{}}

	if hostPort > 0 {
		portBindings[containerSSHPort] = d.sshBinding(hostPort, req.Admit != nil)
	}

	for _, spec := range req.PublishPorts {
		key, err := spec.key()
		if err != nil || key == containerSSHPort || (req.Admit != nil && spec.Protocol != protoTCP) {
			continue
		}

		exposed[key] = struct{}{}
		portBindings[key] = []mobynetwork.PortBinding{{HostIP: parseHostIP(publishHostIP)}}
	}

	hostCfg := &mobycontainer.HostConfig{
		PortBindings:  portBindings,
		AutoRemove:    false,
		RestartPolicy: mobycontainer.RestartPolicy{Name: "no"},
	}

	netName := d.cfg.Network
	if hasNet {
		netName = vn.name
	}

	if netName == "" {
		return hostCfg, nil, exposed
	}

	hostCfg.NetworkMode = mobycontainer.NetworkMode(netName)
	ep := &mobynetwork.EndpointSettings{}

	if hasNet && vn.static {
		if ip, err := netip.ParseAddr(req.PrivateIP); err == nil && ip.Is4() {
			ep.IPAMConfig = &mobynetwork.EndpointIPAMConfig{IPv4Address: ip}
		}
	}

	return hostCfg, &mobynetwork.NetworkingConfig{
		EndpointsConfig: map[string]*mobynetwork.EndpointSettings{netName: ep},
	}, exposed
}

// imdsEndpointEnv is read by every AWS SDK and the CLI in place of 169.254.169.254.
const imdsEndpointEnv = "AWS_EC2_METADATA_SERVICE_ENDPOINT"

func containerEnv(req LaunchRequest) []string {
	if req.MetadataEndpoint == "" {
		return nil
	}

	return []string{imdsEndpointEnv + "=" + req.MetadataEndpoint}
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
	maps.Copy(labels, managedInstanceLabels(req))

	return labels
}

// inspected is what waitForRunning learns about a started container.
type inspected struct {
	ip        string
	published []PublishedPort
	sshPort   int
}

// waitForRunning polls ContainerInspect until the container is running and an IP address is assigned.
func (d *DockerCompute) waitForRunning(ctx context.Context, containerID, preferred string) (inspected, error) {
	timer := time.NewTimer(inspectRetryDelay)
	defer timer.Stop()

	for range inspectMaxRetries {
		res, err := d.api.ContainerInspect(ctx, containerID, mobyclient.ContainerInspectOptions{})
		if err != nil {
			return inspected{}, fmt.Errorf("container inspect: %w", err)
		}

		ip, port := extractInspectAddrs(res.Container, preferred)
		if ip != "" {
			return inspected{ip: ip, sshPort: port, published: publishedOf(res.Container)}, nil
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
			return inspected{}, fmt.Errorf("container inspect: %w", ctx.Err())
		case <-timer.C:
		}
	}

	return inspected{}, errInspectTimeout
}

func publishedOf(c mobycontainer.InspectResponse) []PublishedPort {
	if c.NetworkSettings == nil {
		return nil
	}

	return publishedFrom(c.NetworkSettings.Ports)
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

	for name, ep := range networks {
		if strings.HasPrefix(name, linklocal.NetworkPrefix) {
			continue
		}

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
	d.mounts.dropAll(containerID)
	timeout := stopTimeoutSec
	_, _ = d.api.ContainerStop(ctx, containerID, mobyclient.ContainerStopOptions{Timeout: &timeout})
	_, _ = d.api.ContainerRemove(ctx, containerID, mobyclient.ContainerRemoveOptions{Force: true, RemoveVolumes: true})
	d.releaseSSHPort(port)
}

// reserveSSHPort picks the next free host port in [SSHPortMin, SSHPortMax],
// or 0 when the configured range is empty (Docker assigns an ephemeral one).
func (d *DockerCompute) reserveSSHPort() (int, error) {
	if d.cfg.SSHPortMin <= 0 || d.cfg.SSHPortMax <= 0 || d.cfg.SSHPortMax < d.cfg.SSHPortMin {
		return 0, nil
	}

	d.ports.mu.Lock()
	defer d.ports.mu.Unlock()

	for p := d.cfg.SSHPortMin; p <= d.cfg.SSHPortMax; p++ {
		if _, used := d.ports.used[p]; !used {
			d.ports.used[p] = struct{}{}

			return p, nil
		}
	}

	return 0, errSSHPortRangeEmpty
}

func (d *DockerCompute) releaseSSHPort(port int) {
	if port == 0 {
		return
	}

	d.ports.mu.Lock()
	defer d.ports.mu.Unlock()
	delete(d.ports.used, port)
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
