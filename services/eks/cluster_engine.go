package eks

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

// Engine modes for EKS.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

const (
	// K3sImageRepo is the image repository the cluster engine runs.
	K3sImageRepo = "rancher/k3s"
	// DefaultClusterToken is the static cluster-admin bearer token, like LocalStack's K3D_CLUSTER_TOKEN default.
	DefaultClusterToken = "gopherstack-eks-cluster-token" //nolint:gosec // documented default for a local emulator

	clusterDefaultHost   = "127.0.0.1"
	clusterStartTimeout  = 5 * time.Minute
	clusterProbeInterval = time.Second
	clusterProbeTimeout  = 5 * time.Second
	clusterStopTimeout   = 30 * time.Second
	k3sAPIPort           = 6443
	minTokenLen          = 16
	majorMinorParts      = 2
	firstK3sMinor        = 29
	clusterStartCode     = "CLUSTER_START_FAILED"
	tokenFileName        = "/etc/gopherstack-eks-tokens.csv"
	envAdminAuth         = "GOPHERSTACK_EKS_ADMIN_AUTH"
	envHost              = "GOPHERSTACK_EKS_HOST"
)

// k3sScript writes the static token file, then replaces the shell with the k3s server.
const k3sScript = `printf '%s,admin,admin,"system:masters"\n' "$` + envAdminAuth + `" > ` + tokenFileName +
	` && exec /bin/k3s server --tls-san "$` + envHost + `"` +
	` --disable traefik --disable servicelb --disable metrics-server` +
	` --kube-apiserver-arg=token-auth-file=` + tokenFileName

// k3sTags are the newest published rancher/k3s tags for Kubernetes 1.29 through 1.36, in minor order.
func k3sTags() [8]string {
	return [...]string{
		"v1.29.15-k3s1", "v1.30.14-k3s2", "v1.31.14-k3s1", "v1.32.13-k3s1",
		"v1.33.13-k3s1", "v1.34.12-k3s1", "v1.35.9-k3s1", "v1.36.5-k3s1",
	}
}

var (
	// ErrInvalidClusterEngineConfig is returned when the cluster engine host or token is unusable.
	ErrInvalidClusterEngineConfig = errors.New("invalid EKS cluster engine configuration")
	// ErrDefaultTokenExposed is returned when the public default token would be exposed off-host.
	ErrDefaultTokenExposed = errors.New("EKS_CLUSTER_TOKEN must be set when EKS_CLUSTER_HOST is not loopback")

	tokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~-]+$`)
	hostPattern  = regexp.MustCompile(`^[A-Za-z0-9.:-]+$`)
)

// ClusterEngineEnabled reports whether engine selects docker-backed clusters.
func ClusterEngineEnabled(engine string) bool { return engine == EngineDocker }

// K3sImage maps a Kubernetes minor version to its pinned k3s image; exact is false when it fell back to the default.
func K3sImage(version string) (string, bool) {
	if tag, ok := k3sTag(version); ok {
		return K3sImageRepo + ":" + tag, true
	}

	tag, _ := k3sTag(defaultK8sVersion)

	return K3sImageRepo + ":" + tag, false
}

func k3sTag(version string) (string, bool) {
	parts := strings.Split(version, ".")
	if len(parts) < majorMinorParts || parts[0] != "1" {
		return "", false
	}

	tags := k3sTags()

	minor, err := strconv.Atoi(parts[1])
	if err != nil || minor < firstK3sMinor || minor >= firstK3sMinor+len(tags) {
		return "", false
	}

	return tags[minor-firstK3sMinor], true
}

// ClusterRuntime is the slice of container.Runtime the cluster engine needs.
type ClusterRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
}

// ClusterEngineConfig configures docker-backed EKS clusters.
type ClusterEngineConfig struct {
	Runtime ClusterRuntime
	// Ports hands out host ports; nil falls back to OS-chosen free ports.
	Ports  *portalloc.Allocator
	Logger *slog.Logger
	// Probe returns the server CA (PEM) once the API server at addr answers /readyz for token.
	Probe func(ctx context.Context, addr, token string) ([]byte, error)
	// Host is the address clients use to reach the API server; default 127.0.0.1.
	Host string
	// Token is the static cluster-admin bearer token; default DefaultClusterToken.
	Token string
	// Image overrides the version-mapped k3s image.
	Image string
	// StartTimeout bounds how long a cluster may stay CREATING before FAILED.
	StartTimeout time.Duration
}

type liveCluster struct {
	cancel      context.CancelFunc
	containerID string
	ca          []byte
	port        int
	ready       bool
}

type clusterEngine struct {
	clusters map[string]*liveCluster
	cfg      ClusterEngineConfig
	wg       sync.WaitGroup
}

// EnableClusters switches the backend to docker mode: each CreateCluster starts a single-node k3s container.
func (b *InMemoryBackend) EnableClusters(cfg ClusterEngineConfig) error {
	if cfg.Host == "" {
		cfg.Host = clusterDefaultHost
	}

	defaulted := cfg.Token == ""
	if defaulted {
		cfg.Token = DefaultClusterToken
	}

	if !hostPattern.MatchString(cfg.Host) || !tokenPattern.MatchString(cfg.Token) || len(cfg.Token) < minTokenLen {
		return fmt.Errorf("%w: host must match %s and token %s (min %d chars)",
			ErrInvalidClusterEngineConfig, hostPattern, tokenPattern, minTokenLen)
	}

	if defaulted && container.BindHostFor(cfg.Host) != container.LoopbackHost {
		return ErrDefaultTokenExposed
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = clusterStartTimeout
	}

	if cfg.Probe == nil {
		cfg.Probe = probeK3s
	}

	b.mu.Lock("EnableClusters")
	defer b.mu.Unlock()

	b.clusterEng = &clusterEngine{cfg: cfg, clusters: make(map[string]*liveCluster)}

	return nil
}

func (e *clusterEngine) addr(lc *liveCluster) string {
	return net.JoinHostPort(e.cfg.Host, strconv.Itoa(lc.port))
}

func (e *clusterEngine) image(version string) string {
	if e.cfg.Image != "" {
		return e.cfg.Image
	}

	img, _ := K3sImage(version)

	return img
}

func (e *clusterEngine) spec(name, version string, port int) container.Spec {
	return container.Spec{
		Name:       "gopherstack-eks-" + name + "-" + uuid.NewString()[:8],
		Image:      e.image(version),
		Entrypoint: []string{"/bin/sh", "-c", k3sScript},
		Env:        []string{envAdminAuth + "=" + e.cfg.Token, envHost + "=" + e.cfg.Host},
		Ports: []string{
			container.PortSpec(container.BindHostFor(e.cfg.Host), strconv.Itoa(port), strconv.Itoa(k3sAPIPort)),
		},
		Tmpfs:      []string{"/run", "/var/run"},
		Privileged: true,
	}
}

func (e *clusterEngine) log(ctx context.Context) *slog.Logger {
	if e.cfg.Logger != nil {
		return e.cfg.Logger
	}

	return logger.Load(ctx)
}

func (e *clusterEngine) acquirePort(name string) (int, error) {
	if e.cfg.Ports != nil {
		return e.cfg.Ports.Acquire("eks-" + name) //nolint:wrapcheck // allocator error is self-describing
	}

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("find free port: %w", err)
	}

	defer func() { _ = l.Close() }()

	a, ok := l.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("find free port: %w", io.ErrUnexpectedEOF)
	}

	return a.Port, nil
}

func (e *clusterEngine) releasePort(port int) {
	if e.cfg.Ports != nil && port != 0 {
		_ = e.cfg.Ports.Release(port)
	}
}

func (e *clusterEngine) remove(containerID string, port int) {
	ctx, cancel := context.WithTimeout(context.Background(), clusterStopTimeout)
	defer cancel()

	if err := e.cfg.Runtime.StopAndRemove(ctx, containerID); err != nil {
		e.log(ctx).WarnContext(ctx, "eks: cluster container removal failed", "container", containerID, "error", err)
	}

	e.releasePort(port)
}

func (e *clusterEngine) reap(lcs []*liveCluster) {
	for _, lc := range lcs {
		if lc == nil {
			continue
		}

		lc.cancel()

		if lc.containerID != "" {
			e.wg.Go(func() { e.remove(lc.containerID, lc.port) })
		}
	}
}

func (b *InMemoryBackend) detachClusterLocked(name string) *liveCluster {
	if b.clusterEng == nil {
		return nil
	}

	lc := b.clusterEng.clusters[name]
	delete(b.clusterEng.clusters, name)

	return lc
}

func (b *InMemoryBackend) detachAllClustersLocked() []*liveCluster {
	if b.clusterEng == nil {
		return nil
	}

	lcs := make([]*liveCluster, 0, len(b.clusterEng.clusters))
	for name, lc := range b.clusterEng.clusters {
		lcs = append(lcs, lc)
		delete(b.clusterEng.clusters, name)
	}

	return lcs
}

// reapClustersLocked releases every live cluster; container removal runs in the background.
func (b *InMemoryBackend) reapClustersLocked() {
	if b.clusterEng != nil {
		b.clusterEng.reap(b.detachAllClustersLocked())
	}
}

func (b *InMemoryBackend) closeClusters() {
	b.mu.Lock("closeClusters")
	e := b.clusterEng
	lcs := b.detachAllClustersLocked()
	b.mu.Unlock()

	if e == nil {
		return
	}

	e.reap(lcs)
	e.wg.Wait()

	if c, ok := e.cfg.Runtime.(io.Closer); ok {
		_ = c.Close()
	}
}

// launchClusterLocked registers a pending cluster and starts its container in the background.
func (b *InMemoryBackend) launchClusterLocked(name, version string) {
	e := b.clusterEng
	if e == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	lc := &liveCluster{cancel: cancel}
	e.clusters[name] = lc

	e.wg.Go(func() { b.runCluster(ctx, name, version, lc) })
}

func (b *InMemoryBackend) runCluster(ctx context.Context, name, version string, lc *liveCluster) {
	e := b.clusterEng

	port, err := e.acquirePort(name)
	if err != nil {
		b.failCluster(ctx, name, lc, err)

		return
	}

	id, err := e.cfg.Runtime.CreateAndStart(ctx, e.spec(name, version, port))
	if err != nil {
		e.releasePort(port)
		b.failCluster(ctx, name, lc, err)

		return
	}

	if !b.recordCluster(name, lc, id, port) {
		e.remove(id, port)

		return
	}

	ca, err := e.awaitReady(ctx, e.addr(lc))
	if err != nil {
		b.failCluster(ctx, name, lc, err)

		return
	}

	b.markClusterActive(name, lc, ca)
}

func (b *InMemoryBackend) recordCluster(name string, lc *liveCluster, id string, port int) bool {
	b.mu.Lock("recordCluster")
	defer b.mu.Unlock()

	if b.clusterEng.clusters[name] != lc {
		return false
	}

	lc.containerID = id
	lc.port = port

	return true
}

func (b *InMemoryBackend) markClusterActive(name string, lc *liveCluster, ca []byte) {
	b.mu.Lock("markClusterActive")
	defer b.mu.Unlock()

	if b.clusterEng.clusters[name] != lc {
		return
	}

	lc.ca = ca
	lc.ready = true

	if cl, ok := b.clusters.Get(name); ok && cl.Status == statusCreating {
		cl.Status = statusActive
	}
}

// failCluster marks a still-wanted cluster FAILED and releases its container.
func (b *InMemoryBackend) failCluster(ctx context.Context, name string, lc *liveCluster, cause error) {
	b.mu.Lock("failCluster")

	if b.clusterEng.clusters[name] != lc {
		b.mu.Unlock()

		return
	}

	b.detachClusterLocked(name)

	if cl, ok := b.clusters.Get(name); ok {
		cl.Status = statusFailed
	}

	b.mu.Unlock()

	b.clusterEng.log(ctx).WarnContext(ctx, "eks: cluster failed to start",
		"code", clusterStartCode, "cluster", name, "error", cause)
	b.clusterEng.reap([]*liveCluster{lc})
}

func (e *clusterEngine) awaitReady(ctx context.Context, addr string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(clusterProbeInterval)
	defer ticker.Stop()

	for {
		pctx, pcancel := context.WithTimeout(ctx, clusterProbeTimeout)
		ca, err := e.cfg.Probe(pctx, addr, e.cfg.Token)

		pcancel()

		if err == nil {
			return ca, nil
		}

		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("API server not ready: %w", err)
		case <-ticker.C:
		}
	}
}

// applyLiveEndpoint replaces the synthetic endpoint and CA of a ready docker cluster with the real ones.
func (b *InMemoryBackend) applyLiveEndpoint(cp *Cluster) {
	if b.clusterEng == nil {
		return
	}

	lc, ok := b.clusterEng.clusters[cp.Name]
	if !ok || !lc.ready {
		return
	}

	cp.Endpoint = "https://" + b.clusterEng.addr(lc)
	cp.CertificateAuthority = base64.StdEncoding.EncodeToString(lc.ca)
}

// relaunchClustersLocked starts an empty k3s cluster per restored managed cluster.
func (b *InMemoryBackend) relaunchClustersLocked() {
	if b.clusterEng == nil {
		return
	}

	for _, cl := range b.clusters.All() {
		if cl.ConnectorConfig != nil {
			continue
		}

		cl.Status = statusCreating
		b.launchClusterLocked(cl.Name, cl.Version)
	}
}
