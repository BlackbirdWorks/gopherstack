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
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/safemap"
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
	envCACert            = "GOPHERSTACK_EKS_CA_CERT"
	envCAKey             = "GOPHERSTACK_EKS_CA_KEY"
	k3sTLSDir            = "/var/lib/rancher/k3s/server/tls"
)

// k3sScript writes the token file and the pre-provisioned server CA, then replaces the shell with the k3s server.
const k3sScriptHead = `printf '%s,admin,admin,"system:masters"\n' "$` + envAdminAuth + `" > ` + tokenFileName +
	` && umask 077 && mkdir -p ` + k3sTLSDir +
	` && printf '%s\n' "$` + envCACert + `" > ` + k3sTLSDir + `/server-ca.crt` +
	` && printf '%s\n' "$` + envCAKey + `" > ` + k3sTLSDir + `/server-ca.key` +
	` && unset ` + envCAKey + ` && exec /bin/k3s server --tls-san "$` + envHost + `"` +
	` --disable traefik`

const k3sScriptTail = ` --disable metrics-server --kube-apiserver-arg=token-auth-file=` + tokenFileName

// k3sScript builds the node command; servicelb stays on only when load-balancer ports are published.
func k3sScript(servicelb bool) string {
	if servicelb {
		return k3sScriptHead + k3sScriptTail
	}

	return k3sScriptHead + ` --disable servicelb` + k3sScriptTail
}

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
	Runtime      ClusterRuntime
	Credentials  CredentialResolver
	Ports        *portalloc.Allocator
	Logger       *slog.Logger
	Probe        func(ctx context.Context, addr, token string) ([]byte, error)
	Host         string
	Token        string
	Image        string
	ImageTags    map[string]string
	LBPorts      []int
	StartTimeout time.Duration
}

type liveCluster struct {
	cancel      context.CancelFunc
	api         *k8sAPI
	proxy       *k8sProxy
	kick        chan struct{}
	serverIP    atomic.Pointer[string]
	name        string
	containerID string
	image       string
	joinToken   string
	ca          []byte
	mounts      []string
	lbPorts     []int
	lbHost      []int
	port        int
	proxyPort   int
	dead        atomic.Bool
	ready       bool
}

type clusterEngine struct {
	clusters map[string]*liveCluster
	agents   *safemap.Map[string, *agentNode]
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

	if err := validateK3sImageTags(cfg.ImageTags); err != nil {
		return err
	}

	if err := validateLBPortList(cfg.LBPorts); err != nil {
		return err
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = clusterStartTimeout
	}

	b.mu.Lock("EnableClusters")
	defer b.mu.Unlock()

	b.credentials = cfg.Credentials
	b.clusterEng = &clusterEngine{
		cfg:      cfg,
		clusters: make(map[string]*liveCluster),
		agents:   safemap.New[string, *agentNode]("eks.cluster.agents"),
	}

	return nil
}

func (e *clusterEngine) addr(lc *liveCluster) string {
	return net.JoinHostPort(e.cfg.Host, strconv.Itoa(lc.port))
}

func (e *clusterEngine) spec(name string, lc *liveCluster, port int, lbHost []int, ca *clusterCA) container.Spec {
	bind := container.BindHostFor(e.cfg.Host)
	ports := []string{container.PortSpec(bind, strconv.Itoa(port), strconv.Itoa(k3sAPIPort))}

	for i, p := range lc.lbPorts {
		ports = append(ports, container.PortSpec(bind, strconv.Itoa(lbHost[i]), strconv.Itoa(p)))
	}

	return container.Spec{
		Name:       "gopherstack-eks-" + name + "-" + uuid.NewString()[:8],
		Image:      lc.image,
		Entrypoint: []string{"/bin/sh", "-c", k3sScript(len(lc.lbPorts) > 0)},
		Env: []string{
			envAdminAuth + "=" + e.cfg.Token, envHost + "=" + e.cfg.Host, "K3S_TOKEN=" + lc.joinToken,
			envCACert + "=" + string(ca.certPEM), envCAKey + "=" + string(ca.keyPEM),
		},
		Ports:      ports,
		Mounts:     lc.mounts,
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
	return portalloc.AcquireFrom(e.cfg.Ports, "eks-"+name)
}

func (e *clusterEngine) releaseAll(port int, lbHost []int) {
	e.releasePort(port)

	for _, p := range lbHost {
		e.releasePort(p)
	}
}

func (e *clusterEngine) releasePort(port int) {
	portalloc.ReleaseTo(e.cfg.Ports, port)
}

func (e *clusterEngine) remove(containerID string, ports ...int) {
	ctx, cancel := context.WithTimeout(context.Background(), clusterStopTimeout)
	defer cancel()

	if err := e.cfg.Runtime.StopAndRemove(ctx, containerID); err != nil {
		e.log(ctx).WarnContext(ctx, "eks: cluster container removal failed", "container", containerID, "error", err)
	}

	for _, p := range ports {
		e.releasePort(p)
	}
}

func (e *clusterEngine) reap(lcs []*liveCluster) {
	for _, lc := range lcs {
		if lc == nil {
			continue
		}

		lc.dead.Store(true)
		lc.cancel()
		e.reapAgents(lc)

		if lc.proxy != nil {
			lc.proxy.close()
			e.releasePort(lc.proxyPort)
		}

		if lc.api != nil {
			lc.api.close()
		}

		if lc.containerID != "" {
			e.wg.Go(func() { e.remove(lc.containerID, append([]int{lc.port}, lc.lbHost...)...) })
		}
	}
}

func (e *clusterEngine) reapAgents(lc *liveCluster) {
	for _, a := range e.agents.Values() {
		if a.cluster != lc.name {
			continue
		}

		e.agents.Delete(a.containerID)
		e.wg.Go(func() { e.remove(a.containerID, 0) })
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
	e.agents.Close()

	if c, ok := e.cfg.Runtime.(io.Closer); ok {
		_ = c.Close()
	}
}

// launchClusterLocked registers a pending cluster and starts its container in the background.
func (b *InMemoryBackend) launchClusterLocked(name, version string, kv map[string]string) error {
	e := b.clusterEng
	if e == nil {
		return nil
	}

	opts, image, err := e.resolveLaunch(version, kv)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	lc := &liveCluster{
		cancel: cancel, name: name, kick: make(chan struct{}, 1), image: image, joinToken: randomHex32(),
		mounts: opts.mounts, lbPorts: opts.lbPorts,
	}
	e.clusters[name] = lc

	e.wg.Go(func() { b.runCluster(ctx, name, version, lc) })

	return nil
}

func (b *InMemoryBackend) runCluster(ctx context.Context, name, version string, lc *liveCluster) {
	e := b.clusterEng

	ca, err := newClusterCA(name)
	if err != nil {
		b.failCluster(ctx, name, lc, err)

		return
	}

	var (
		port   int
		lbHost []int
	)

	id, err := container.CreateAndStartRetry(ctx, e.cfg.Runtime,
		func() (container.Spec, error) {
			p, perr := e.acquirePort(name)
			if perr != nil {
				return container.Spec{}, perr
			}

			port = p
			lbHost = nil

			for range lc.lbPorts {
				hp, lerr := e.acquirePort(name)
				if lerr != nil {
					e.releaseAll(port, lbHost)

					return container.Spec{}, lerr
				}

				lbHost = append(lbHost, hp)
			}

			return e.spec(name, lc, p, lbHost, ca), nil
		},
		func() { e.releaseAll(port, lbHost) })
	if err != nil {
		b.failCluster(ctx, name, lc, err)

		return
	}

	if !b.recordCluster(name, lc, id, port, lbHost) {
		e.remove(id, append([]int{port}, lbHost...)...)

		return
	}

	caPEM, err := e.awaitReady(ctx, e.addr(lc), ca.certPEM)
	if err != nil {
		b.failCluster(ctx, name, lc, err)

		return
	}

	api, err := newK8sAPI(e.addr(lc), e.cfg.Token, ca.certPEM)
	if err != nil {
		b.failCluster(ctx, name, lc, err)

		return
	}

	proxy, err := b.startProxy(name, ca, api)
	if err != nil {
		api.close()
		b.failCluster(ctx, name, lc, err)

		return
	}

	if !b.markClusterActive(name, lc, caPEM, api, proxy) {
		proxy.close()
		e.releasePort(proxy.port)
		api.close()

		return
	}

	e.wg.Go(func() { b.runReconciler(ctx, name, version, lc) })
}

func (b *InMemoryBackend) recordCluster(name string, lc *liveCluster, id string, port int, lbHost []int) bool {
	b.mu.Lock("recordCluster")
	defer b.mu.Unlock()

	if b.clusterEng.clusters[name] != lc {
		return false
	}

	lc.containerID = id
	lc.port = port
	lc.lbHost = lbHost

	return true
}

func (b *InMemoryBackend) markClusterActive(
	name string, lc *liveCluster, ca []byte, api *k8sAPI, proxy *k8sProxy,
) bool {
	b.mu.Lock("markClusterActive")
	defer b.mu.Unlock()

	if b.clusterEng.clusters[name] != lc {
		return false
	}

	lc.ca = ca
	lc.api = api
	lc.proxy = proxy
	lc.proxyPort = proxy.port
	lc.ready = true

	if cl, ok := b.clusters.Get(name); ok && cl.Status == statusCreating {
		cl.Status = statusActive
	}

	return true
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

	for _, ng := range b.nodegroupsByCluster.Get(name) {
		if ng.Status == statusCreating {
			ng.Status = statusCreateFail
		}
	}

	b.mu.Unlock()

	b.clusterEng.log(ctx).WarnContext(ctx, "eks: cluster failed to start",
		"code", clusterStartCode, "cluster", name, "error", cause)
	b.clusterEng.reap([]*liveCluster{lc})
}

func (e *clusterEngine) probe(ctx context.Context, addr string, ca []byte) ([]byte, error) {
	if e.cfg.Probe != nil {
		return e.cfg.Probe(ctx, addr, e.cfg.Token)
	}

	return ca, probeK3s(ctx, addr, e.cfg.Token, ca)
}

func (e *clusterEngine) awaitReady(ctx context.Context, addr string, ca []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	ticker := time.NewTicker(clusterProbeInterval)
	defer ticker.Stop()

	for {
		pctx, pcancel := context.WithTimeout(ctx, clusterProbeTimeout)
		got, err := e.probe(pctx, addr, ca)

		pcancel()

		if err == nil {
			return got, nil
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
	b.applyExternalEndpoint(cp)

	if b.clusterEng == nil {
		return
	}

	lc, ok := b.clusterEng.clusters[cp.Name]
	if !ok || !lc.ready {
		return
	}

	cp.Endpoint = "https://" + net.JoinHostPort(b.clusterEng.cfg.Host, strconv.Itoa(lc.proxyPort))
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

		var kv map[string]string
		if cl.Tags != nil {
			kv = cl.Tags.Clone()
		}

		if err := b.launchClusterLocked(cl.Name, cl.Version, kv); err != nil {
			cl.Status = statusFailed
			b.clusterEng.log(context.Background()).Warn("eks: cluster not relaunched",
				"code", clusterStartCode, "cluster", cl.Name, "error", err)

			continue
		}

		b.resetEngineResourcesLocked(cl.Name)
	}
}

// resetEngineResourcesLocked returns restored nodegroups and engine-backed add-ons to CREATING so the fresh cluster
// re-earns ACTIVE from real nodes and deployments.
func (b *InMemoryBackend) resetEngineResourcesLocked(cluster string) {
	for _, ng := range b.nodegroupsByCluster.Get(cluster) {
		if ng.Status == statusActive || ng.Status == statusCreateFail {
			ng.Status = statusCreating
		}
	}

	for _, a := range b.addonsByCluster.Get(cluster) {
		if engineBackedAddon(a.AddonName) && a.Status != statusDeleting {
			a.Status = statusCreating
		}
	}
}
