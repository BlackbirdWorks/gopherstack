package neptune

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

// Engine modes for Neptune.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
	// EngineEmbedded serves Gremlin, openCypher and Bolt in-process over one graph; no container runtime needed.
	EngineEmbedded = "embedded"
)

const (
	engineDefaultHost   = "127.0.0.1"
	engineStartTimeout  = 3 * time.Minute
	engineProbeInterval = 500 * time.Millisecond
	engineProbeTimeout  = 5 * time.Second
	engineStopTimeout   = 30 * time.Second

	engineStartFailedCode = "NEPTUNE_ENGINE_START_FAILED"
	gremlinContainerPort  = 8182
	gremlinImageRepo      = "tinkerpop/gremlin-server:"
	defaultGremlinVersion = "3.7.3"
	versionPartsKept      = 3

	statusFailed   = "failed"
	statusStopping = "stopping"
	statusStarting = "starting"
)

// EngineModeConfig exposes the Neptune engine mode (stub, embedded or docker).
type EngineModeConfig interface {
	GetNeptuneEngine() string
}

// EngineRuntime is the slice of the container runtime the Gremlin engine needs.
type EngineRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
}

// EngineConfig configures docker-backed Neptune clusters.
type EngineConfig struct {
	Runtime      EngineRuntime
	Ports        *portalloc.Allocator
	Probe        func(ctx context.Context, addr string) error
	Host         string
	StartTimeout time.Duration
	OpenCypher   bool
	// InProcessGremlin serves Gremlin from the cluster's shared graph instead of a Gremlin Server container.
	InProcessGremlin bool
}

type liveCluster struct {
	ctx         context.Context
	cancel      context.CancelFunc
	front       *cypherFront
	relay       *sgfilter.Proxy
	graph       *opencypher.Graph
	key         string
	region      string
	id          string
	version     string
	containerID string
	port        int
	innerPort   int
	svcPort     int
	ready       bool
}

type dbEngine struct {
	units map[string]*liveCluster
	cfg   EngineConfig
	wg    sync.WaitGroup
}

type unitOp int

const (
	opStop unitOp = iota
	opStart
)

// EnableEngine switches the backend to docker mode: each cluster gets a real Gremlin Server container.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) {
	if cfg.Host == "" {
		cfg.Host = engineDefaultHost
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = engineStartTimeout
	}

	if cfg.Probe == nil {
		cfg.Probe = ProbeGremlin
	}

	b.mu.Lock("EnableEngine")
	defer b.mu.Unlock()

	b.engine = &dbEngine{cfg: cfg, units: make(map[string]*liveCluster)}
}

// gremlinTag maps a Neptune engine version to the TinkerPop release that stands in for it.
func gremlinTag(engineVersion string) string {
	parts := strings.SplitN(engineVersion, ".", versionPartsKept)
	if len(parts) < 2 { //nolint:mnd // major.minor
		return defaultGremlinVersion
	}

	switch parts[0] + "." + parts[1] {
	case "1.0", "1.1":
		return "3.4.13"
	case "1.2":
		return "3.5.7"
	case "1.3":
		return "3.6.8"
	default:
		return defaultGremlinVersion
	}
}

func (e *dbEngine) spec(lb *liveCluster) container.Spec {
	bind, port := container.BindHostFor(e.cfg.Host), lb.port
	if lb.innerPort != 0 {
		bind, port = container.LoopbackHost, lb.innerPort
	}

	return container.Spec{
		Name:  "gopherstack-neptune-" + uuid.NewString()[:12],
		Image: gremlinImageRepo + lb.version,
		Ports: []string{container.PortSpec(bind, strconv.Itoa(port), strconv.Itoa(gremlinContainerPort))},
	}
}

// addr is where the Gremlin Server answers: the published port, or the loopback inner port behind the front.
func (e *dbEngine) addr(lb *liveCluster) string {
	if lb.innerPort != 0 {
		return net.JoinHostPort(container.LoopbackHost, strconv.Itoa(lb.innerPort))
	}

	return net.JoinHostPort(e.cfg.Host, strconv.Itoa(lb.port))
}

func (e *dbEngine) acquirePort() (int, error) {
	return portalloc.AcquireFrom(e.cfg.Ports, "neptune")
}

func (e *dbEngine) releasePort(port int) {
	portalloc.ReleaseTo(e.cfg.Ports, port)
}

func (e *dbEngine) remove(containerID string, ports ...int) {
	ctx, cancel := context.WithTimeout(context.Background(), engineStopTimeout)
	defer cancel()

	if containerID != "" {
		if err := e.cfg.Runtime.StopAndRemove(ctx, containerID); err != nil {
			logger.Load(ctx).WarnContext(ctx, "neptune: engine container removal failed",
				"container", containerID, "error", err)
		}
	}

	for _, port := range ports {
		e.releasePort(port)
	}
}

func (e *dbEngine) reap(lbs ...*liveCluster) {
	for _, lb := range lbs {
		if lb == nil {
			continue
		}

		lb.cancel()

		id, port, inner, front, relay := lb.containerID, lb.port, lb.innerPort, lb.front, lb.relay
		e.wg.Go(func() {
			closeFronts(front, relay)

			e.remove(id, port, inner)
		})
	}
}

func (b *InMemoryBackend) unitLocked(region, id string) *liveCluster {
	if b.engine == nil {
		return nil
	}

	return b.engine.units[regionKey(region, id)]
}

func (b *InMemoryBackend) attachedLocked(lb *liveCluster) bool {
	return b.engine != nil && b.engine.units[lb.key] == lb
}

func (b *InMemoryBackend) dropUnitLocked(region, id string) {
	if b.engine == nil {
		return
	}

	key := regionKey(region, id)
	if lb, ok := b.engine.units[key]; ok {
		delete(b.engine.units, key)
		b.engine.reap(lb)
	}

	delete(b.graphs, key)
}

func (b *InMemoryBackend) dropAllUnitsLocked() {
	if b.engine == nil {
		return
	}

	for key, lb := range b.engine.units {
		delete(b.engine.units, key)
		b.engine.reap(lb)
	}
}

func (b *InMemoryBackend) rekeyUnitLocked(region, oldID, newID string) {
	if lb := b.unitLocked(region, oldID); lb != nil {
		delete(b.engine.units, lb.key)
		oldKey := lb.key
		lb.id, lb.key = newID, regionKey(region, newID)
		b.engine.units[lb.key] = lb

		if g, ok := b.graphs[oldKey]; ok {
			delete(b.graphs, oldKey)
			b.graphs[lb.key] = g
		}
	}
}

// provisionClusterLocked starts the Gremlin Server container behind c; the cluster stays creating until it answers.
func (b *InMemoryBackend) provisionClusterLocked(c *DBCluster) {
	e := b.engine
	if e == nil || b.closed {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	lb := &liveCluster{
		ctx: ctx, cancel: cancel, key: regionKey(c.region, c.DBClusterIdentifier), region: c.region,
		id: c.DBClusterIdentifier, version: gremlinTag(c.EngineVersion),
		graph:   b.graphLocked(regionKey(c.region, c.DBClusterIdentifier)),
		svcPort: cmp.Or(c.Port, defaultNeptunePort),
	}

	if old, ok := e.units[lb.key]; ok {
		e.reap(old)
	}

	e.units[lb.key] = lb
	c.Status = statusCreating
	c.readyAt = time.Time{}

	if e.cfg.InProcessGremlin {
		e.wg.Go(func() { b.runEmbeddedUnit(lb) })

		return
	}

	e.wg.Go(func() { b.runUnit(lb) })
}

var errUnitDetached = errors.New("engine unit detached")

func (b *InMemoryBackend) runUnit(lb *liveCluster) {
	e := b.engine

	var port, inner int

	id, err := container.CreateAndStartRetry(lb.ctx, e.cfg.Runtime,
		func() (container.Spec, error) {
			var perr error

			if port, perr = e.acquirePort(); perr != nil {
				return container.Spec{}, perr
			}

			inner = 0

			if e.cfg.OpenCypher || b.enforcing() {
				if inner, perr = e.acquirePort(); perr != nil {
					e.releasePort(port)

					return container.Spec{}, perr
				}
			}

			if !b.recordPort(lb, port, inner) {
				e.releasePort(port)
				e.releasePort(inner)

				return container.Spec{}, errUnitDetached
			}

			return e.spec(lb), nil
		},
		func() {
			e.releasePort(port)
			e.releasePort(inner)
			b.recordPort(lb, 0, 0)
		})
	if err != nil {
		if !errors.Is(err, errUnitDetached) {
			b.failUnit(lb)
		}

		return
	}

	front, relay, err := b.startFronts(lb, port, inner)
	if err != nil {
		e.remove(id, port, inner)
		b.failUnit(lb)

		return
	}

	if !b.recordContainer(lb, id, front, relay) {
		closeFronts(front, relay)

		e.remove(id, port, inner)

		return
	}

	b.awaitAndMark(lb)
}

// runEmbeddedUnit opens the cluster's listener over its shared graph; no container is involved.
func (b *InMemoryBackend) runEmbeddedUnit(lb *liveCluster) {
	e := b.engine

	port, err := e.acquirePort()
	if err != nil {
		b.failUnit(lb)

		return
	}

	if !b.recordPort(lb, port, 0) {
		e.releasePort(port)

		return
	}

	front, err := startCypherFront(e.cfg.Host, port, "", lb.graph, b.clusterAdmit(lb), b.clusterAuthFn(lb))
	if err != nil {
		e.releasePort(port)
		b.failUnit(lb)

		return
	}

	if !b.recordContainer(lb, "", front, nil) {
		front.Close()
		e.releasePort(port)

		return
	}

	b.markUnitReady(lb)
}

func (b *InMemoryBackend) recordPort(lb *liveCluster, port, inner int) bool {
	b.mu.Lock("neptuneEnginePort")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.port, lb.innerPort = port, inner

	return true
}

func (b *InMemoryBackend) recordContainer(
	lb *liveCluster, id string, front *cypherFront, relay *sgfilter.Proxy,
) bool {
	b.mu.Lock("neptuneEngineContainer")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.containerID, lb.front, lb.relay = id, front, relay

	return true
}

func (b *InMemoryBackend) awaitAndMark(lb *liveCluster) {
	b.mu.RLock("neptuneEngineAddr")
	addr := b.engine.addr(lb)
	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(lb.ctx, b.engine.cfg.StartTimeout)
	defer cancel()

	if b.engine.awaitReady(ctx, addr) != nil {
		b.failUnit(lb)

		return
	}

	b.markUnitReady(lb)
}

func (e *dbEngine) awaitReady(ctx context.Context, addr string) error {
	ticker := time.NewTicker(engineProbeInterval)
	defer ticker.Stop()

	for {
		pctx, pcancel := context.WithTimeout(ctx, engineProbeTimeout)
		err := e.cfg.Probe(pctx, addr)

		pcancel()

		if err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("engine not reachable: %w", err)
		case <-ticker.C:
		}
	}
}

func (b *InMemoryBackend) unitMembersLocked(lb *liveCluster, onInstance func(*DBInstance), onCluster func(*DBCluster)) {
	if c, ok := b.clusterGet(lb.region, lb.id); ok {
		onCluster(c)
	}

	for _, inst := range b.instancesInRegion(lb.region) {
		if inst.DBClusterIdentifier == lb.id {
			onInstance(inst)
		}
	}
}

func transitional(status string) bool {
	return status == statusCreating || status == statusStarting
}

func (b *InMemoryBackend) applyLiveEndpointsLocked(lb *liveCluster) {
	host := b.engine.cfg.Host

	b.unitMembersLocked(lb, func(inst *DBInstance) {
		inst.Endpoint, inst.Port = host, lb.port

		if transitional(inst.DBInstanceStatus) {
			inst.DBInstanceStatus = clusterStatusAvailable
		}
	}, func(c *DBCluster) {
		c.Endpoint, c.ReaderEndpoint, c.Port = host, host, lb.port

		if transitional(c.Status) {
			c.Status = clusterStatusAvailable
		}
	})
}

func (b *InMemoryBackend) markUnitReady(lb *liveCluster) {
	b.mu.Lock("neptuneEngineReady")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return
	}

	lb.ready = true
	if lb.front != nil {
		lb.front.up.Store(true)
	}

	b.applyLiveEndpointsLocked(lb)
}

// reapplyLiveLocked restores the published endpoint on c after a modify may have overwritten it.
func (b *InMemoryBackend) reapplyLiveLocked(c *DBCluster) {
	if lb := b.unitLocked(c.region, c.DBClusterIdentifier); lb != nil && lb.ready {
		c.Endpoint, c.ReaderEndpoint, c.Port = b.engine.cfg.Host, b.engine.cfg.Host, lb.port
	}
}

// reapplyLiveInstanceLocked restores the published endpoint on inst after a modify may have overwritten it.
func (b *InMemoryBackend) reapplyLiveInstanceLocked(inst *DBInstance) {
	if lb := b.unitLocked(inst.region, inst.DBClusterIdentifier); lb != nil && lb.ready {
		inst.Endpoint, inst.Port = b.engine.cfg.Host, lb.port
	}
}

func (b *InMemoryBackend) failUnit(lb *liveCluster) {
	b.mu.Lock("neptuneEngineFail")

	if !b.attachedLocked(lb) {
		b.mu.Unlock()

		return
	}

	delete(b.engine.units, lb.key)
	b.unitMembersLocked(lb, func(inst *DBInstance) {
		inst.DBInstanceStatus = statusFailed
	}, func(c *DBCluster) {
		c.Status = statusFailed
	})
	b.mu.Unlock()

	logger.Load(lb.ctx).WarnContext(lb.ctx, "neptune: cluster engine failed to start", "code", engineStartFailedCode)
	b.engine.reap(lb)
}

// runUnitOpLocked stops or starts lb's container in the background; the caller sets the cluster status.
func (b *InMemoryBackend) runUnitOpLocked(lb *liveCluster, op unitOp) {
	lb.ready = false
	if lb.front != nil {
		lb.front.up.Store(false)
	}

	id := lb.containerID

	b.engine.wg.Go(func() { b.execUnitOp(lb, id, op) })
}

func (b *InMemoryBackend) execUnitOp(lb *liveCluster, id string, op unitOp) {
	if b.engine.cfg.InProcessGremlin {
		if op == opStop {
			b.markUnitStopped(lb)
		} else {
			b.markUnitReady(lb)
		}

		return
	}

	rt := b.engine.cfg.Runtime
	ctx, cancel := context.WithTimeout(lb.ctx, engineStopTimeout)

	defer cancel()

	if op == opStop {
		if err := rt.StopContainer(ctx, id); err != nil {
			b.failUnit(lb)

			return
		}

		b.markUnitStopped(lb)

		return
	}

	if err := rt.StartContainer(ctx, id); err != nil {
		b.failUnit(lb)

		return
	}

	b.awaitAndMark(lb)
}

func (b *InMemoryBackend) markUnitStopped(lb *liveCluster) {
	b.mu.Lock("neptuneEngineStopped")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return
	}

	if c, ok := b.clusterGet(lb.region, lb.id); ok && c.Status == statusStopping {
		c.Status = clusterStatusStopped
	}
}

// stopUnitLocked moves c to stopping while its container stops; false means no engine backs c.
func (b *InMemoryBackend) stopUnitLocked(c *DBCluster) bool {
	lb := b.unitLocked(c.region, c.DBClusterIdentifier)
	if lb == nil {
		return false
	}

	c.Status = statusStopping
	b.runUnitOpLocked(lb, opStop)

	return true
}

// startUnitLocked moves c to starting while its container restarts; false means no engine backs c.
func (b *InMemoryBackend) startUnitLocked(c *DBCluster) bool {
	lb := b.unitLocked(c.region, c.DBClusterIdentifier)
	if lb == nil {
		return false
	}

	c.Status = statusStarting
	b.runUnitOpLocked(lb, opStart)

	return true
}

// relaunchClustersLocked starts a container per restored cluster (the Gremlin graph is not persisted).
func (b *InMemoryBackend) relaunchClustersLocked() {
	if b.engine == nil {
		return
	}

	for _, c := range b.clusters.All() {
		if c.Status == statusDeleting {
			continue
		}

		b.provisionClusterLocked(c)

		for _, inst := range b.instancesInRegion(c.region) {
			if inst.DBClusterIdentifier == c.DBClusterIdentifier && inst.DBInstanceStatus != statusDeleting {
				inst.DBInstanceStatus = statusCreating
				inst.readyAt = time.Time{}
			}
		}
	}
}

// joinEngineLocked points a new cluster member at the cluster's live engine, or leaves it creating until it is up.
func (b *InMemoryBackend) joinEngineLocked(inst *DBInstance) {
	lb := b.unitLocked(inst.region, inst.DBClusterIdentifier)
	if lb == nil {
		return
	}

	inst.readyAt = time.Time{}

	if !lb.ready {
		inst.DBInstanceStatus = statusCreating

		return
	}

	inst.Endpoint, inst.Port = b.engine.cfg.Host, lb.port
}

// Close removes every cluster container and waits for in-flight work to finish.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")

	if b.closed {
		b.mu.Unlock()

		return
	}

	b.closed = true
	b.dropAllUnitsLocked()
	e := b.engine
	b.mu.Unlock()

	if e == nil {
		return
	}

	e.wg.Wait()

	if c, ok := e.cfg.Runtime.(io.Closer); ok {
		_ = c.Close()
	}
}

// Shutdown removes every cluster container.
func (h *Handler) Shutdown(_ context.Context) {
	if c, ok := h.Backend.(interface{ Close() }); ok {
		c.Close()
	}
}
