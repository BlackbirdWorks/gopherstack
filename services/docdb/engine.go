package docdb

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
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
)

// Engine modes for DocDB.
const (
	EngineStub   = "stub"
	EngineDocker = "docker"
)

const (
	engineDefaultHost   = "127.0.0.1"
	engineStartTimeout  = 3 * time.Minute
	engineProbeInterval = 500 * time.Millisecond
	engineProbeTimeout  = 5 * time.Second
	engineStopTimeout   = 30 * time.Second
	engineOpTimeout     = 30 * time.Second

	engineStartFailedCode = "DOCDB_ENGINE_START_FAILED"
	engineRandPassBytes   = 12
	mongoContainerPort    = 27017
	mongoShell            = "mongo"
	defaultMongoVersion   = "5.0"

	statusFailed   = "failed"
	statusStopping = "stopping"
	statusStarting = "starting"
)

// EngineModeConfig exposes the DocDB engine mode (stub or docker).
type EngineModeConfig interface {
	GetDocDBEngine() string
}

// EngineRuntime is the slice of the container runtime the MongoDB engine needs.
type EngineRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
	ExecEnv(ctx context.Context, containerID string, cmd, env []string) (container.ExecResult, error)
}

// PasswordChange describes a master password change inside a running MongoDB container.
type PasswordChange struct {
	ContainerID string
	Version     string
	User        string
	OldPassword string
	NewPassword string
}

// EngineConfig configures docker-backed DocDB clusters.
type EngineConfig struct {
	Runtime      EngineRuntime
	Ports        *portalloc.Allocator
	Probe        func(ctx context.Context, addr string) error
	SetPassword  func(ctx context.Context, change PasswordChange) error
	Host         string
	StartTimeout time.Duration
	TLS          bool
}

type liveCluster struct {
	ctx         context.Context
	cancel      context.CancelFunc
	proxy       *tlsProxy
	key         string
	region      string
	id          string
	version     string
	user        string
	password    string
	containerID string
	port        int
	inner       int
	svcPort     int
	ready       bool
}

type dbEngine struct {
	tls   *tlsAuthority
	units map[string]*liveCluster
	cfg   EngineConfig
	wg    sync.WaitGroup
}

type unitOp int

const (
	opStop unitOp = iota
	opStart
)

// EnableEngine switches the backend to docker mode: each cluster gets a real MongoDB container.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) {
	if cfg.Host == "" {
		cfg.Host = engineDefaultHost
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = engineStartTimeout
	}

	var authority *tlsAuthority

	if cfg.TLS {
		var err error
		if authority, err = newTLSAuthority(); err != nil {
			logger.Load(context.Background()).Warn("docdb: TLS unavailable; clusters stay plaintext", "error", err)
		}
	}

	if cfg.Probe == nil {
		cfg.Probe = probeMongo
	}

	if cfg.SetPassword == nil {
		cfg.SetPassword = func(ctx context.Context, c PasswordChange) error {
			return execSetPassword(ctx, cfg.Runtime, c)
		}
	}

	b.mu.Lock("EnableEngine")
	defer b.mu.Unlock()

	b.engine = &dbEngine{cfg: cfg, tls: authority, units: make(map[string]*liveCluster)}
}

// mongoTag maps a DocDB major.minor to the official mongo image tag that stands in for it.
func mongoTag(engineVersion string) string {
	major, rest, _ := strings.Cut(engineVersion, ".")
	minor, _, _ := strings.Cut(rest, ".")

	switch major + "." + minor {
	case "3.6", "4.0":
		return "4.4"
	default:
		return defaultMongoVersion
	}
}

func (e *dbEngine) spec(lb *liveCluster) container.Spec {
	bindHost, hostPort := container.BindHostFor(e.cfg.Host), lb.port
	if lb.inner != 0 {
		bindHost, hostPort = container.LoopbackHost, lb.inner
	}

	spec := container.Spec{
		Name:  "gopherstack-docdb-" + uuid.NewString()[:12],
		Image: "mongo:" + lb.version,
		Env:   []string{"MONGO_INITDB_ROOT_USERNAME=" + lb.user, "MONGO_INITDB_ROOT_PASSWORD=" + lb.password},
		Ports: []string{container.PortSpec(bindHost, strconv.Itoa(hostPort), strconv.Itoa(mongoContainerPort))},
	}

	return spec
}

func (e *dbEngine) addr(lb *liveCluster) string {
	return net.JoinHostPort(e.cfg.Host, strconv.Itoa(lb.port))
}

func (e *dbEngine) acquirePort() (int, error) {
	return portalloc.AcquireFrom(e.cfg.Ports, "docdb")
}

func (e *dbEngine) releasePort(port int) {
	portalloc.ReleaseTo(e.cfg.Ports, port)
}

func (e *dbEngine) remove(containerID string, port, inner int, proxy *tlsProxy) {
	ctx, cancel := context.WithTimeout(context.Background(), engineStopTimeout)
	defer cancel()

	if containerID != "" {
		if err := e.cfg.Runtime.StopAndRemove(ctx, containerID); err != nil {
			logger.Load(ctx).WarnContext(ctx, "docdb: engine container removal failed",
				"container", containerID, "error", err)
		}
	}

	if proxy != nil {
		proxy.Close()
	}

	e.releasePort(port)
	e.releasePort(inner)
}

func (e *dbEngine) reap(lbs ...*liveCluster) {
	for _, lb := range lbs {
		if lb == nil {
			continue
		}

		lb.cancel()

		id, port, inner, proxy := lb.containerID, lb.port, lb.inner, lb.proxy
		e.wg.Go(func() { e.remove(id, port, inner, proxy) })
	}
}

func randomPassword() string {
	buf := make([]byte, engineRandPassBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
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
		lb.id, lb.key = newID, regionKey(region, newID)
		b.engine.units[lb.key] = lb
	}
}

// provisionClusterLocked starts the MongoDB container behind c; the cluster stays creating until it answers.
func (b *InMemoryBackend) provisionClusterLocked(c *DBCluster, password string) {
	e := b.engine
	if e == nil || b.closed {
		return
	}

	if password == "" {
		password = randomPassword()
	}

	ctx, cancel := context.WithCancel(context.Background())
	lb := &liveCluster{
		ctx: ctx, cancel: cancel, key: regionKey(c.region, c.DBClusterIdentifier), region: c.region,
		id: c.DBClusterIdentifier, version: mongoTag(c.EngineVersion), user: c.MasterUsername, password: password,
		svcPort: cmp.Or(c.Port, defaultDocDBPort),
	}
	e.units[lb.key] = lb
	c.Status = statusCreating
	c.readyAt = time.Time{}

	e.wg.Go(func() { b.runUnit(lb) })
}

var errUnitDetached = errors.New("engine unit detached")

func (b *InMemoryBackend) runUnit(lb *liveCluster) {
	e := b.engine

	var (
		port, inner int
		proxy       *tlsProxy
	)

	id, err := container.CreateAndStartRetry(lb.ctx, e.cfg.Runtime,
		func() (container.Spec, error) {
			var perr error

			if port, inner, perr = e.acquirePorts(b.enforcing()); perr != nil {
				return container.Spec{}, perr
			}

			if !b.recordPorts(lb, port, inner) {
				e.releasePort(port)
				e.releasePort(inner)

				return container.Spec{}, errUnitDetached
			}

			if proxy, perr = b.startProxy(lb); perr != nil {
				e.releasePort(port)
				e.releasePort(inner)
				b.recordPorts(lb, 0, 0)

				return container.Spec{}, perr
			}

			return e.spec(lb), nil
		},
		func() {
			if proxy != nil {
				proxy.Close()
				proxy = nil
			}

			e.releasePort(port)
			e.releasePort(inner)
			b.recordPorts(lb, 0, 0)
		})
	if err != nil {
		if !errors.Is(err, errUnitDetached) {
			b.failUnit(lb)
		}

		return
	}

	if !b.recordContainer(lb, id, proxy) {
		e.remove(id, port, inner, proxy)

		return
	}

	b.awaitAndMark(lb)
}

// acquirePorts reserves the published port and, when TLS or a security-group relay fronts the container, the
// loopback port behind it.
func (e *dbEngine) acquirePorts(relay bool) (int, int, error) {
	port, err := e.acquirePort()
	if err != nil || (e.tls == nil && !relay) {
		return port, 0, err
	}

	inner, err := e.acquirePort()
	if err != nil {
		e.releasePort(port)

		return 0, 0, err
	}

	return port, inner, nil
}

func (b *InMemoryBackend) recordPorts(lb *liveCluster, port, inner int) bool {
	b.mu.Lock("docdbEnginePort")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.port, lb.inner = port, inner

	return true
}

// startProxy opens the TLS-terminating listener in front of lb's container; nil when TLS is off for the engine.
func (b *InMemoryBackend) startProxy(lb *liveCluster) (*tlsProxy, error) {
	e := b.engine
	admit := b.clusterAdmit(lb)

	if e.tls == nil && admit == nil {
		return nil, nil //nolint:nilnil // no proxy is a valid state
	}

	b.mu.RLock("docdbEngineProxy")
	require := b.clusterTLSByID(lb.region, lb.id)
	b.mu.RUnlock()

	upstream := net.JoinHostPort(container.LoopbackHost, strconv.Itoa(lb.inner))
	if e.tls == nil {
		return startPlainProxy(e.cfg.Host, lb.port, upstream, admit)
	}

	return e.tls.startProxy(e.cfg.Host, lb.port, upstream, require, admit)
}

func (b *InMemoryBackend) recordContainer(lb *liveCluster, id string, proxy *tlsProxy) bool {
	b.mu.Lock("docdbEngineContainer")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.containerID, lb.proxy = id, proxy

	return true
}

func (b *InMemoryBackend) awaitAndMark(lb *liveCluster) {
	b.mu.RLock("docdbEngineAddr")
	addr := b.engine.addr(lb)
	if lb.inner != 0 {
		addr = net.JoinHostPort(container.LoopbackHost, strconv.Itoa(lb.inner))
	}

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
			inst.DBInstanceStatus = statusAvailable
		}
	}, func(c *DBCluster) {
		c.Endpoint, c.ReaderEndpoint, c.Port = host, host, lb.port

		if transitional(c.Status) {
			c.Status = statusAvailable
		}
	})
}

func (b *InMemoryBackend) markUnitReady(lb *liveCluster) {
	b.mu.Lock("docdbEngineReady")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return
	}

	lb.ready = true
	b.applyLiveEndpointsLocked(lb)
}

// reapplyLiveLocked restores the published endpoint on c after a modify may have overwritten it.
func (b *InMemoryBackend) reapplyLiveLocked(c *DBCluster) {
	if lb := b.unitLocked(c.region, c.DBClusterIdentifier); lb != nil && lb.ready {
		c.Endpoint, c.ReaderEndpoint, c.Port = b.engine.cfg.Host, b.engine.cfg.Host, lb.port
	}
}

func (b *InMemoryBackend) failUnit(lb *liveCluster) {
	b.mu.Lock("docdbEngineFail")

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

	logger.Load(lb.ctx).WarnContext(lb.ctx, "docdb: cluster engine failed to start", "code", engineStartFailedCode)
	b.engine.reap(lb)
}

// runUnitOpLocked stops or starts lb's container in the background; the caller sets the cluster status.
func (b *InMemoryBackend) runUnitOpLocked(lb *liveCluster, op unitOp) {
	lb.ready = false
	id := lb.containerID

	b.engine.wg.Go(func() { b.execUnitOp(lb, id, op) })
}

func (b *InMemoryBackend) execUnitOp(lb *liveCluster, id string, op unitOp) {
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
	b.mu.Lock("docdbEngineStopped")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return
	}

	if c, ok := b.clusterGet(lb.region, lb.id); ok && c.Status == statusStopping {
		c.Status = statusStopped
	}
}

// applyMasterPassword changes the master password inside the running container; a no-op when none backs the cluster.
func (b *InMemoryBackend) applyMasterPassword(region, id, newPassword string) error {
	if newPassword == "" {
		return nil
	}

	b.mu.RLock("docdbEnginePassword")

	lb := b.unitLocked(region, id)
	if lb == nil || lb.password == newPassword {
		b.mu.RUnlock()

		return nil
	}

	change := PasswordChange{
		ContainerID: lb.containerID, Version: lb.version, User: lb.user,
		OldPassword: lb.password, NewPassword: newPassword,
	}
	ready, set := lb.ready, b.engine.cfg.SetPassword
	b.mu.RUnlock()

	if !ready {
		return fmt.Errorf("%w: cluster %s is not available to change its master password", ErrInvalidClusterState, id)
	}

	ctx, cancel := context.WithTimeout(lb.ctx, engineOpTimeout)
	defer cancel()

	if err := set(ctx, change); err != nil {
		logger.Load(ctx).WarnContext(ctx, "docdb: applying master password failed", "error", err)

		return fmt.Errorf("%w: the master password could not be applied to cluster %s", ErrInvalidClusterState, id)
	}

	b.mu.Lock("docdbEnginePasswordSet")
	lb.password = newPassword
	b.mu.Unlock()

	return nil
}

// changePasswordScript reads credentials from the process env so they never reach argv.
const changePasswordScript = "var a=db.getSiblingDB('admin');" +
	"if(!a.auth(_getEnv('GS_USER'),_getEnv('GS_OLD_PW'))){quit(2)}" +
	"a.changeUserPassword(_getEnv('GS_USER'),_getEnv('GS_NEW_PW'))"

func execSetPassword(ctx context.Context, rt EngineRuntime, c PasswordChange) error {
	cmd := []string{mongoShell, "--quiet", "--eval", changePasswordScript}
	env := []string{"GS_USER=" + c.User, "GS_OLD_PW=" + c.OldPassword, "GS_NEW_PW=" + c.NewPassword}

	res, err := container.ExecWithEnv(ctx, rt, c.ContainerID, cmd, env)
	if err != nil {
		if errors.Is(err, container.ErrExecUnsupported) {
			return fmt.Errorf("exec mongo shell: %w", err)
		}

		return errExecFailed
	}

	if res.ExitCode != 0 {
		return fmt.Errorf("%w: exit %d", errShellFailed, res.ExitCode)
	}

	return nil
}

// relaunchClustersLocked starts an empty container per restored cluster; passwords are not persisted, so
// each relaunched engine gets a random one until ModifyDBCluster sets a new one.
func (b *InMemoryBackend) relaunchClustersLocked() {
	if b.engine == nil {
		return
	}

	for _, c := range b.clusters.All() {
		if c.Status == statusDeleting {
			continue
		}

		b.provisionClusterLocked(c, "")

		for _, inst := range b.instancesInRegion(c.region) {
			if inst.DBClusterIdentifier == c.DBClusterIdentifier && inst.DBInstanceStatus != statusDeleting {
				inst.DBInstanceStatus = statusCreating
			}
		}
	}
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
func (h *Handler) Shutdown(_ context.Context) { h.Backend.Close() }

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
