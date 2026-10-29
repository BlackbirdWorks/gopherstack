package eks

import (
	"context"
	"encoding/base64"
	"errors"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
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
	K3sImageRepo = k3sengine.ImageRepo
	// DefaultClusterToken is the static cluster-admin bearer token, like LocalStack's K3D_CLUSTER_TOKEN default.
	DefaultClusterToken = k3sengine.DefaultToken

	clusterStopTimeout = 30 * time.Second
	k3sAPIPort         = k3sengine.APIPort
	majorMinorParts    = 2
	clusterStartCode   = "CLUSTER_START_FAILED"
)

var (
	// ErrInvalidClusterEngineConfig is returned when the cluster engine host or token is unusable.
	ErrInvalidClusterEngineConfig = k3sengine.ErrInvalidConfig
	// ErrDefaultTokenExposed is returned when the public default token would be exposed off-host.
	ErrDefaultTokenExposed = errors.New("EKS_CLUSTER_TOKEN must be set when EKS_CLUSTER_HOST is not loopback")
)

// ClusterEngineEnabled reports whether engine selects docker-backed clusters.
func ClusterEngineEnabled(engine string) bool { return engine == EngineDocker }

// K3sImage maps a Kubernetes minor version to its pinned k3s image; exact is false when it fell back to the default.
func K3sImage(version string) (string, bool) { return k3sengine.Image(version, defaultK8sVersion) }

// ClusterRuntime is the slice of container.Runtime the cluster engine needs.
type ClusterRuntime = k3sengine.Runtime

// ClusterEngineConfig configures docker-backed EKS clusters.
type ClusterEngineConfig struct {
	Runtime          ClusterRuntime
	Credentials      CredentialResolver
	Ports            *portalloc.Allocator
	Logger           *slog.Logger
	Probe            func(ctx context.Context, addr, token string) ([]byte, error)
	Host             string
	Token            string
	Image            string
	ImageTags        map[string]string
	LBPorts          []int
	VolumeMountAllow []string
	StartTimeout     time.Duration
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
	core     *k3sengine.Engine
	clusters map[string]*liveCluster
	agents   *safemap.Map[string, *agentNode]
	cfg      ClusterEngineConfig
	wg       sync.WaitGroup
}

// EnableClusters switches the backend to docker mode: each CreateCluster starts a single-node k3s container.
func (b *InMemoryBackend) EnableClusters(cfg ClusterEngineConfig) error {
	core, err := k3sengine.New(k3sengine.Config{
		Runtime: cfg.Runtime, Ports: cfg.Ports, Logger: cfg.Logger, Probe: cfg.Probe, Prefix: "eks",
		Host: cfg.Host, Token: cfg.Token, StartTimeout: cfg.StartTimeout,
	})
	if err != nil {
		if errors.Is(err, k3sengine.ErrDefaultTokenExposed) {
			return ErrDefaultTokenExposed
		}

		return err
	}

	cfg.Host, cfg.Token = core.Host(), core.Token()

	if err = validateK3sImageTags(cfg.ImageTags); err != nil {
		return err
	}

	if err = validateMountAllow(cfg.VolumeMountAllow); err != nil {
		return err
	}

	if err = validateLBPortList(cfg.LBPorts); err != nil {
		return err
	}

	b.mu.Lock("EnableClusters")
	defer b.mu.Unlock()

	b.credentials = cfg.Credentials
	b.clusterEng = &clusterEngine{
		cfg:      cfg,
		core:     core,
		clusters: make(map[string]*liveCluster),
		agents:   safemap.New[string, *agentNode]("eks.cluster.agents"),
	}

	return nil
}

func (e *clusterEngine) addr(lc *liveCluster) string { return e.core.Addr(lc.port) }

func (e *clusterEngine) log(ctx context.Context) *slog.Logger { return e.core.Log(ctx) }

func (e *clusterEngine) releasePort(port int) { e.core.ReleasePort(port) }

func (e *clusterEngine) remove(containerID string, ports ...int) {
	e.core.Remove(containerID, ports...)
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
	e.core.Close()
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

	inst, err := e.core.Launch(ctx,
		k3sengine.Node{Name: name, Image: lc.image, JoinToken: lc.joinToken, Mounts: lc.mounts, LBPorts: lc.lbPorts},
		func(i *k3sengine.Instance) bool { return b.recordCluster(name, lc, i.ContainerID, i.Port, i.LBHost) })
	if err != nil {
		if !errors.Is(err, k3sengine.ErrSuperseded) {
			b.failCluster(ctx, name, lc, err)
		}

		return
	}

	ca := &clusterCA{certPEM: inst.CA.CertPEM, keyPEM: inst.CA.KeyPEM}

	caPEM, err := e.core.AwaitReady(ctx, e.addr(lc), ca.certPEM)
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
