// Package linklocal makes the AWS link-local service addresses (169.254.170.2 for ECS task
// credentials, 169.254.169.254 for EC2 IMDS) reachable from Docker containers without root.
//
// A labelled bridge network owns the address; a tiny busybox relay container holds it and
// forwards plain HTTP to the emulator. Containers join the network as a secondary endpoint.
package linklocal

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"strings"
	"sync"
	"time"

	mobycontainer "github.com/moby/moby/api/types/container"
	mobynetwork "github.com/moby/moby/api/types/network"
	mobyclient "github.com/moby/moby/client"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	// DefaultImage is the relay image; it must already exist locally, it is never pulled.
	DefaultImage = "alpine:3.21"

	// PeerHeader carries the connecting container's address to the emulator.
	PeerHeader = "X-Gopherstack-Peer"

	// IMDSPathPrefix is where the IMDS relay forwards; the EC2 handler resolves the instance from PeerHeader.
	IMDSPathPrefix = "/_gopherstack/ec2/imds/linklocal"

	labelKey    = "gopherstack.linklocal"
	labelConfig = "gopherstack.linklocal.config"
	// NetworkPrefix prefixes every link-local network and relay container name.
	NetworkPrefix = "gopherstack-linklocal-"
	relayPort     = "80"
	retryAfter    = time.Minute
	cacheTTL      = 30 * time.Second
	startWait     = 10 * time.Second
	pollEvery     = 100 * time.Millisecond
	gwPriority    = -1
	relayScript   = "printf '%s\\n' \"$LL_SCRIPT\" > /relay.sh && chmod +x /relay.sh && " +
		"exec nc -lk -p " + relayPort + " -e /relay.sh"
)

// ErrImageMissing is returned when the relay image is not present locally.
var ErrImageMissing = errors.New("linklocal: relay image not present locally")

// ErrNetworkRemove is returned when the link-local network cannot be removed.
var ErrNetworkRemove = errors.New("linklocal: network remove")

// ErrNotRunning is returned when the relay container does not stay up.
var ErrNotRunning = errors.New("linklocal: relay container not running")

//go:embed relay.sh
var relaySource string

// Spec describes one link-local network and its relay.
type Spec struct {
	// Name distinguishes the network and relay container ("ecs", "imds").
	Name string
	// Image is the relay image (busybox with nc); defaults to DefaultImage.
	Image string
	// Subnet is the link-local network CIDR.
	Subnet netip.Prefix
	// ProxyIP is the address the relay holds inside Subnet.
	ProxyIP netip.Addr
	// UpstreamHost and UpstreamPort are where the relay forwards, as seen from inside the network.
	UpstreamHost string
	UpstreamPort string
	// PathPrefix is prepended to every forwarded request path.
	PathPrefix string
	// OwnerLabel is the container label naming the owning entity, used to recover owners after a restart.
	OwnerLabel string
}

// ECSSpec is the 169.254.170.2 task-credentials relay.
func ECSSpec(image, host, port string) Spec {
	return Spec{
		Name:         "ecs",
		Image:        image,
		Subnet:       netip.MustParsePrefix("169.254.170.0/24"),
		ProxyIP:      netip.MustParseAddr("169.254.170.2"),
		UpstreamHost: host,
		UpstreamPort: port,
	}
}

// IMDSSpec is the 169.254.169.254 instance-metadata relay.
func IMDSSpec(image, host, port, ownerLabel string) Spec {
	return Spec{
		Name:         "imds",
		Image:        image,
		Subnet:       netip.MustParsePrefix("169.254.169.0/24"),
		ProxyIP:      netip.MustParseAddr("169.254.169.254"),
		UpstreamHost: host,
		UpstreamPort: port,
		PathPrefix:   IMDSPathPrefix,
		OwnerLabel:   ownerLabel,
	}
}

// API is the Docker surface the manager uses.
type API interface {
	ImageInspect(
		ctx context.Context, image string, opts ...mobyclient.ImageInspectOption,
	) (mobyclient.ImageInspectResult, error)
	NetworkCreate(
		ctx context.Context, name string, opts mobyclient.NetworkCreateOptions,
	) (mobyclient.NetworkCreateResult, error)
	NetworkList(ctx context.Context, opts mobyclient.NetworkListOptions) (mobyclient.NetworkListResult, error)
	NetworkInspect(
		ctx context.Context, id string, opts mobyclient.NetworkInspectOptions,
	) (mobyclient.NetworkInspectResult, error)
	NetworkConnect(
		ctx context.Context, id string, opts mobyclient.NetworkConnectOptions,
	) (mobyclient.NetworkConnectResult, error)
	NetworkRemove(
		ctx context.Context, id string, opts mobyclient.NetworkRemoveOptions,
	) (mobyclient.NetworkRemoveResult, error)
	ContainerCreate(
		ctx context.Context, opts mobyclient.ContainerCreateOptions,
	) (mobyclient.ContainerCreateResult, error)
	ContainerStart(
		ctx context.Context, id string, opts mobyclient.ContainerStartOptions,
	) (mobyclient.ContainerStartResult, error)
	ContainerList(ctx context.Context, opts mobyclient.ContainerListOptions) (mobyclient.ContainerListResult, error)
	ContainerInspect(
		ctx context.Context, id string, opts mobyclient.ContainerInspectOptions,
	) (mobyclient.ContainerInspectResult, error)
	ContainerRemove(
		ctx context.Context, id string, opts mobyclient.ContainerRemoveOptions,
	) (mobyclient.ContainerRemoveResult, error)
}

// Manager owns one link-local network and relay. It is safe for concurrent use.
type Manager struct {
	lastTry  time.Time
	cacheAt  time.Time
	api      API
	lastErr  error
	cache    map[string]cacheEntry
	owners   map[string]string
	netName  string
	hashSpec string
	spec     Spec
	mu       sync.Mutex
	ready    bool
}

type cacheEntry struct {
	containerID string
}

// New returns a Manager over api. Nothing touches Docker until Active or Attach.
func New(api API, spec Spec) *Manager {
	if spec.Image == "" {
		spec.Image = DefaultImage
	}

	m := &Manager{
		api:    api,
		spec:   spec,
		cache:  map[string]cacheEntry{},
		owners: map[string]string{},
	}
	m.netName = NetworkPrefix + spec.Name
	m.hashSpec = specHash(spec)

	return m
}

// NewFromEnv returns a Manager over the Docker daemon named by DOCKER_HOST.
func NewFromEnv(spec Spec) (*Manager, error) {
	cli, err := mobyclient.New(mobyclient.FromEnv)
	if err != nil {
		return nil, fmt.Errorf("linklocal: docker client: %w", err)
	}

	return New(cli, spec), nil
}

// NetworkName is the Docker network holding the relay.
func (m *Manager) NetworkName() string { return m.netName }

// Active reports whether the relay is up, creating it on first use. A failure is logged once
// and not retried for a minute.
func (m *Manager) Active(ctx context.Context) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.ensureLocked(ctx) == nil
}

func (m *Manager) ensureLocked(ctx context.Context) error {
	if m.ready {
		return nil
	}

	if m.lastErr != nil && time.Since(m.lastTry) < retryAfter {
		return m.lastErr
	}

	m.lastTry = time.Now()

	if err := m.ensure(ctx); err != nil {
		if m.lastErr == nil || m.lastErr.Error() != err.Error() {
			logger.Load(ctx).WarnContext(ctx, "link-local relay unavailable; falling back to direct endpoints",
				"relay", m.spec.Name, "image", m.spec.Image, "error", err)
		}

		m.lastErr = err

		return err
	}

	m.ready, m.lastErr = true, nil

	return nil
}

func (m *Manager) ensure(ctx context.Context) error {
	if _, err := m.api.ImageInspect(ctx, m.spec.Image); err != nil {
		return fmt.Errorf("%w: %s: %w", ErrImageMissing, m.spec.Image, err)
	}

	if err := m.ensureNetwork(ctx); err != nil {
		return err
	}

	return m.ensureRelay(ctx)
}

func (m *Manager) labels() map[string]string {
	return map[string]string{labelKey: m.spec.Name, labelConfig: m.hashSpec}
}

func (m *Manager) filter() mobyclient.Filters {
	return mobyclient.Filters{}.Add("label", labelKey+"="+m.spec.Name)
}

func (m *Manager) ensureNetwork(ctx context.Context) error {
	res, err := m.api.NetworkList(ctx, mobyclient.NetworkListOptions{Filters: m.filter()})
	if err != nil {
		return fmt.Errorf("linklocal: network list: %w", err)
	}

	for _, n := range res.Items {
		if n.Name == m.netName && n.Labels[labelKey] == m.spec.Name {
			return nil
		}
	}

	_, err = m.api.NetworkCreate(ctx, m.netName, mobyclient.NetworkCreateOptions{
		Driver: "bridge",
		IPAM:   &mobynetwork.IPAM{Config: []mobynetwork.IPAMConfig{{Subnet: m.spec.Subnet}}},
		Labels: map[string]string{labelKey: m.spec.Name},
	})
	if err != nil {
		return fmt.Errorf("linklocal: network create %s (%s): %w", m.netName, m.spec.Subnet, err)
	}

	return nil
}

func (m *Manager) ensureRelay(ctx context.Context) error {
	res, err := m.api.ContainerList(ctx, mobyclient.ContainerListOptions{All: true, Filters: m.filter()})
	if err != nil {
		return fmt.Errorf("linklocal: container list: %w", err)
	}

	for _, c := range res.Items {
		if c.Labels[labelConfig] == m.hashSpec {
			return m.startRelay(ctx, c.ID)
		}

		_, _ = m.api.ContainerRemove(ctx, c.ID, mobyclient.ContainerRemoveOptions{Force: true})
	}

	return m.createRelay(ctx)
}

func (m *Manager) createRelay(ctx context.Context) error {
	created, err := m.api.ContainerCreate(ctx, mobyclient.ContainerCreateOptions{
		Name: m.netName,
		Config: &mobycontainer.Config{
			Image:  m.spec.Image,
			Cmd:    []string{"sh", "-c", relayScript},
			Labels: m.labels(),
			Env: []string{
				"LL_SCRIPT=" + relaySource,
				"LL_HOST=" + m.spec.UpstreamHost,
				"LL_PORT=" + m.spec.UpstreamPort,
				"LL_PREFIX=" + m.spec.PathPrefix,
			},
		},
		HostConfig: &mobycontainer.HostConfig{
			NetworkMode:   mobycontainer.NetworkMode(m.netName),
			RestartPolicy: mobycontainer.RestartPolicy{Name: mobycontainer.RestartPolicyUnlessStopped},
		},
		NetworkingConfig: &mobynetwork.NetworkingConfig{
			EndpointsConfig: map[string]*mobynetwork.EndpointSettings{
				m.netName: {IPAMConfig: &mobynetwork.EndpointIPAMConfig{IPv4Address: m.spec.ProxyIP}},
			},
		},
	})
	if err != nil {
		return fmt.Errorf("linklocal: relay create: %w", err)
	}

	return m.startRelay(ctx, created.ID)
}

func (m *Manager) startRelay(ctx context.Context, id string) error {
	if _, err := m.api.ContainerStart(ctx, id, mobyclient.ContainerStartOptions{}); err != nil {
		return fmt.Errorf("linklocal: relay start: %w", err)
	}

	deadline := time.Now().Add(startWait)

	for {
		info, err := m.api.ContainerInspect(ctx, id, mobyclient.ContainerInspectOptions{})
		if err == nil && info.Container.State != nil && info.Container.State.Running {
			return nil
		}

		if time.Now().After(deadline) || ctx.Err() != nil {
			return fmt.Errorf("%w: %s", ErrNotRunning, id)
		}

		time.Sleep(pollEvery)
	}
}

// Attach joins containerID (created, started or not) to the link-local network as a secondary
// endpoint that never carries the default route. owner is recorded for OwnerOf.
func (m *Manager) Attach(ctx context.Context, containerID, owner string) error {
	m.mu.Lock()
	err := m.ensureLocked(ctx)
	m.mu.Unlock()

	if err != nil {
		return err
	}

	_, err = m.api.NetworkConnect(ctx, m.netName, mobyclient.NetworkConnectOptions{
		Container:      containerID,
		EndpointConfig: &mobynetwork.EndpointSettings{GwPriority: gwPriority},
	})
	if err != nil {
		return fmt.Errorf("linklocal: attach %s: %w", containerID, err)
	}

	if owner != "" {
		m.mu.Lock()
		m.owners[containerID] = owner
		m.mu.Unlock()
	}

	return nil
}

// Release forgets containerID's owner and any cached address.
func (m *Manager) Release(containerID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.owners, containerID)

	for ip, e := range m.cache {
		if e.containerID == containerID {
			delete(m.cache, ip)
		}
	}
}

// OwnerOf resolves the owner of the container holding link-local address ip.
func (m *Manager) OwnerOf(ctx context.Context, ip string) (string, bool) {
	id, ok := m.containerAt(ctx, ip)
	if !ok {
		return "", false
	}

	m.mu.Lock()
	owner, known := m.owners[id]
	m.mu.Unlock()

	if known {
		return owner, true
	}

	return m.ownerFromLabel(ctx, id)
}

func (m *Manager) ownerFromLabel(ctx context.Context, id string) (string, bool) {
	if m.spec.OwnerLabel == "" {
		return "", false
	}

	info, err := m.api.ContainerInspect(ctx, id, mobyclient.ContainerInspectOptions{})
	if err != nil || info.Container.Config == nil {
		return "", false
	}

	owner := info.Container.Config.Labels[m.spec.OwnerLabel]
	if owner == "" {
		return "", false
	}

	m.mu.Lock()
	m.owners[id] = owner
	m.mu.Unlock()

	return owner, true
}

func (m *Manager) containerAt(ctx context.Context, ip string) (string, bool) {
	m.mu.Lock()
	if e, ok := m.cache[ip]; ok && time.Since(m.cacheAt) < cacheTTL {
		m.mu.Unlock()

		return e.containerID, true
	}
	m.mu.Unlock()

	res, err := m.api.NetworkInspect(ctx, m.netName, mobyclient.NetworkInspectOptions{Verbose: true})
	if err != nil {
		return "", false
	}

	fresh := map[string]cacheEntry{}

	for id, ep := range res.Network.Containers {
		if p, perr := netip.ParsePrefix(ep.IPv4Address.String()); perr == nil {
			fresh[p.Addr().String()] = cacheEntry{containerID: id}
		}
	}

	m.mu.Lock()
	m.cache, m.cacheAt = fresh, time.Now()
	m.mu.Unlock()

	e, ok := fresh[ip]

	return e.containerID, ok
}

// Remove deletes the relay container and network. It fails if other containers are still attached.
func (m *Manager) Remove(ctx context.Context) error {
	m.mu.Lock()
	m.ready = false
	m.mu.Unlock()

	res, err := m.api.ContainerList(ctx, mobyclient.ContainerListOptions{All: true, Filters: m.filter()})
	if err != nil {
		return fmt.Errorf("linklocal: container list: %w", err)
	}

	for _, c := range res.Items {
		_, _ = m.api.ContainerRemove(ctx, c.ID, mobyclient.ContainerRemoveOptions{Force: true})
	}

	nets, err := m.api.NetworkList(ctx, mobyclient.NetworkListOptions{Filters: m.filter()})
	if err != nil {
		return fmt.Errorf("linklocal: network list: %w", err)
	}

	var errs []string

	for _, n := range nets.Items {
		if _, rmErr := m.api.NetworkRemove(ctx, n.ID, mobyclient.NetworkRemoveOptions{}); rmErr != nil {
			errs = append(errs, rmErr.Error())
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("%w: %s", ErrNetworkRemove, strings.Join(errs, "; "))
	}

	return nil
}

func specHash(s Spec) string {
	return hex.EncodeToString(hashStrings(s.Image, s.Subnet.String(), s.ProxyIP.String(),
		s.UpstreamHost, s.UpstreamPort, s.PathPrefix, relaySource, relayScript))
}

func hashStrings(parts ...string) []byte {
	h := sha256.New()

	for _, p := range parts {
		_, _ = h.Write([]byte(p))
		_, _ = h.Write([]byte{0})
	}

	return h.Sum(nil)[:8]
}

// Close releases the Docker client; it leaves the relay and network running.
func (m *Manager) Close() error {
	if c, ok := m.api.(io.Closer); ok {
		return c.Close()
	}

	return nil
}
