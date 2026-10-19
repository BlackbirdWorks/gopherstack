package redshift

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

// Engine modes for Redshift.
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

	engineStartFailedCode = "REDSHIFT_ENGINE_START_FAILED"
	engineRandPassBytes   = 12
	pgContainerPort       = 5432
	defaultEngineImage    = "postgres:17"

	clusterStatusRebooting   = "rebooting"
	clusterStatusRestoring   = "restoring"
	clusterStatusEngineError = "hardware-failure"
)

// EngineModeConfig exposes the Redshift engine mode (stub or docker).
type EngineModeConfig interface {
	GetRedshiftEngine() string
}

// EngineRuntime is the slice of the container runtime the Postgres engine needs.
type EngineRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
}

// EngineLogin identifies a database login on a cluster's live engine.
type EngineLogin struct {
	Addr     string
	User     string
	Password string
	DBName   string
}

// PasswordChange describes a master password change on a running engine.
type PasswordChange struct {
	Login       EngineLogin
	NewPassword string
}

// EngineConfig configures docker-backed Redshift clusters.
type EngineConfig struct {
	Runtime EngineRuntime
	// Ports hands out host ports; nil falls back to OS-chosen free ports.
	Ports *portalloc.Allocator
	// Probe returns nil once the engine accepts the login; nil runs SELECT 1 over the Postgres wire.
	Probe func(ctx context.Context, login EngineLogin) error
	// SetPassword changes the master password; nil issues ALTER USER over the Postgres wire.
	SetPassword func(ctx context.Context, change PasswordChange) error
	// Host is the address clients use to reach published ports; default 127.0.0.1.
	Host string
	// Image is the Postgres image standing in for Redshift; default postgres:17.
	Image string
	// StartTimeout bounds how long a cluster may stay unreachable before it is marked failed.
	StartTimeout time.Duration
}

type liveCluster struct {
	ctx         context.Context
	cancel      context.CancelFunc
	id          string
	user        string
	password    string
	dbName      string
	containerID string
	port        int
	ready       bool
}

type dbEngine struct {
	units  map[string]*liveCluster
	cfg    EngineConfig
	wg     sync.WaitGroup
	shared bool
}

type unitOp int

const (
	opStop unitOp = iota
	opStart
	opRestart
)

// EnableEngine switches the backend to docker mode: each cluster gets a real Postgres container.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) {
	b.enableEngine(cfg, false)
}

func (b *InMemoryBackend) enableEngine(cfg EngineConfig, shared bool) {
	if cfg.Host == "" {
		cfg.Host = engineDefaultHost
	}

	if cfg.Image == "" {
		cfg.Image = defaultEngineImage
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = engineStartTimeout
	}

	if cfg.Probe == nil {
		cfg.Probe = probePostgres
	}

	if cfg.SetPassword == nil {
		cfg.SetPassword = alterMasterPassword
	}

	b.mu.Lock("EnableEngine")
	defer b.mu.Unlock()

	b.engine = &dbEngine{cfg: cfg, units: make(map[string]*liveCluster), shared: shared}
}

func (b *InMemoryBackend) engineConfig() (EngineConfig, bool) {
	b.mu.RLock("engineConfig")
	defer b.mu.RUnlock()

	if b.engine == nil {
		return EngineConfig{}, false
	}

	return b.engine.cfg, true
}

func (e *dbEngine) spec(lb *liveCluster) container.Spec {
	env := []string{"POSTGRES_USER=" + lb.user, "POSTGRES_PASSWORD=" + lb.password}
	if lb.dbName != "" {
		env = append(env, "POSTGRES_DB="+lb.dbName)
	}

	return container.Spec{
		Name:  "gopherstack-redshift-" + uuid.NewString()[:12],
		Image: e.cfg.Image,
		Env:   env,
		Ports: []string{container.PortSpec(
			container.BindHostFor(e.cfg.Host), strconv.Itoa(lb.port), strconv.Itoa(pgContainerPort))},
	}
}

func (e *dbEngine) login(lb *liveCluster) EngineLogin {
	return EngineLogin{
		Addr: net.JoinHostPort(e.cfg.Host, strconv.Itoa(lb.port)),
		User: lb.user, Password: lb.password, DBName: lb.dbName,
	}
}

func (e *dbEngine) acquirePort() (int, error) {
	if e.cfg.Ports != nil {
		return e.cfg.Ports.Acquire("redshift") //nolint:wrapcheck // allocator error is self-describing
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

func (e *dbEngine) releasePort(port int) {
	if e.cfg.Ports != nil && port != 0 {
		_ = e.cfg.Ports.Release(port)
	}
}

func (e *dbEngine) remove(containerID string, port int) {
	ctx, cancel := context.WithTimeout(context.Background(), engineStopTimeout)
	defer cancel()

	if containerID != "" {
		if err := e.cfg.Runtime.StopAndRemove(ctx, containerID); err != nil {
			logger.Load(ctx).WarnContext(ctx, "redshift: engine container removal failed",
				"container", containerID, "error", err)
		}
	}

	e.releasePort(port)
}

func (e *dbEngine) reap(lbs ...*liveCluster) {
	for _, lb := range lbs {
		if lb == nil {
			continue
		}

		lb.cancel()

		id, port := lb.containerID, lb.port
		e.wg.Go(func() { e.remove(id, port) })
	}
}

func randomPassword() string {
	buf := make([]byte, engineRandPassBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

func (b *InMemoryBackend) unitLocked(id string) *liveCluster {
	if b.engine == nil {
		return nil
	}

	return b.engine.units[id]
}

func (b *InMemoryBackend) attachedLocked(lb *liveCluster) bool {
	return b.engine != nil && b.engine.units[lb.id] == lb
}

func (b *InMemoryBackend) dropUnitLocked(id string) {
	if b.engine == nil {
		return
	}

	if lb, ok := b.engine.units[id]; ok {
		delete(b.engine.units, id)
		b.engine.reap(lb)
	}
}

func (b *InMemoryBackend) dropAllUnitsLocked() {
	if b.engine == nil {
		return
	}

	for id, lb := range b.engine.units {
		delete(b.engine.units, id)
		b.engine.reap(lb)
	}
}

func (b *InMemoryBackend) rekeyUnitLocked(oldID, newID string) {
	if lb := b.unitLocked(oldID); lb != nil {
		delete(b.engine.units, oldID)
		lb.id = newID
		b.engine.units[newID] = lb
	}
}

// provisionClusterLocked starts the Postgres container behind c; the cluster stays creating until it answers.
func (b *InMemoryBackend) provisionClusterLocked(c *Cluster, password string) {
	e := b.engine
	if e == nil || b.closed {
		return
	}

	if password == "" {
		password = randomPassword()
	}

	ctx, cancel := context.WithCancel(context.Background())
	lb := &liveCluster{
		ctx: ctx, cancel: cancel, id: c.ClusterIdentifier,
		user: c.MasterUsername, password: password, dbName: c.DBName,
	}
	e.units[lb.id] = lb
	c.Status = clusterStatusCreating
	delete(b.clusterTransitions, c.ClusterIdentifier)

	e.wg.Go(func() { b.runUnit(lb) })
}

func (b *InMemoryBackend) runUnit(lb *liveCluster) {
	e := b.engine

	port, err := e.acquirePort()
	if err != nil {
		b.failUnit(lb)

		return
	}

	if !b.recordPort(lb, port) {
		e.releasePort(port)

		return
	}

	id, err := e.cfg.Runtime.CreateAndStart(lb.ctx, e.spec(lb))
	if err != nil {
		b.failUnit(lb)

		return
	}

	if !b.recordContainer(lb, id) {
		e.remove(id, port)

		return
	}

	b.awaitAndMark(lb)
}

func (b *InMemoryBackend) recordPort(lb *liveCluster, port int) bool {
	b.mu.Lock("redshiftEnginePort")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.port = port

	return true
}

func (b *InMemoryBackend) recordContainer(lb *liveCluster, id string) bool {
	b.mu.Lock("redshiftEngineContainer")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.containerID = id

	return true
}

func (b *InMemoryBackend) awaitAndMark(lb *liveCluster) {
	b.mu.RLock("redshiftEngineLogin")
	login := b.engine.login(lb)
	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(lb.ctx, b.engine.cfg.StartTimeout)
	defer cancel()

	if b.engine.awaitReady(ctx, login) != nil {
		b.failUnit(lb)

		return
	}

	b.markUnitReady(lb)
}

func (e *dbEngine) awaitReady(ctx context.Context, login EngineLogin) error {
	ticker := time.NewTicker(engineProbeInterval)
	defer ticker.Stop()

	for {
		pctx, pcancel := context.WithTimeout(ctx, engineProbeTimeout)
		err := e.cfg.Probe(pctx, login)

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

func transitional(status string) bool {
	switch status {
	case clusterStatusCreating, clusterStatusResuming, clusterStatusRebooting, clusterStatusRestoring:
		return true
	default:
		return false
	}
}

func (b *InMemoryBackend) markUnitReady(lb *liveCluster) {
	b.mu.Lock("redshiftEngineReady")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return
	}

	if strings.HasPrefix(lb.id, workgroupUnitPrefix) {
		b.markWorkgroupReadyLocked(lb)

		return
	}

	lb.ready = true

	c, ok := b.clusters.Get(lb.id)
	if !ok {
		return
	}

	host := b.engine.cfg.Host
	if b.dnsRegistrar != nil && c.Endpoint != host {
		b.dnsRegistrar.Deregister(c.Endpoint)
	}

	c.Endpoint, c.Port = host, lb.port

	if transitional(c.Status) {
		c.Status = clusterStatusAvailable
	}
}

// reapplyLiveLocked restores the published endpoint on c after a modify may have overwritten it.
func (b *InMemoryBackend) reapplyLiveLocked(c *Cluster) {
	if lb := b.unitLocked(c.ClusterIdentifier); lb != nil && lb.ready {
		c.Endpoint, c.Port = b.engine.cfg.Host, lb.port
	}
}

func (b *InMemoryBackend) liveEndpointLocked(c *Cluster) bool {
	lb := b.unitLocked(c.ClusterIdentifier)

	return lb != nil && lb.ready
}

func (b *InMemoryBackend) failUnit(lb *liveCluster) {
	b.mu.Lock("redshiftEngineFail")

	if !b.attachedLocked(lb) {
		b.mu.Unlock()

		return
	}

	delete(b.engine.units, lb.id)

	if strings.HasPrefix(lb.id, workgroupUnitPrefix) {
		b.failWorkgroupLocked(lb)
		b.mu.Unlock()
		b.engine.reap(lb)

		return
	}

	if c, ok := b.clusters.Get(lb.id); ok {
		c.Status = clusterStatusEngineError
	}

	b.mu.Unlock()

	logger.Load(lb.ctx).WarnContext(lb.ctx, "redshift: cluster engine failed to start", "code", engineStartFailedCode)
	b.engine.reap(lb)
}

// runUnitOpLocked stops, starts or restarts lb's container in the background; the caller sets the cluster status.
func (b *InMemoryBackend) runUnitOpLocked(lb *liveCluster, op unitOp) {
	lb.ready = false
	id := lb.containerID

	b.engine.wg.Go(func() { b.execUnitOp(lb, id, op) })
}

func (b *InMemoryBackend) execUnitOp(lb *liveCluster, id string, op unitOp) {
	rt := b.engine.cfg.Runtime
	ctx, cancel := context.WithTimeout(lb.ctx, engineStopTimeout)

	defer cancel()

	if op != opStart {
		if err := rt.StopContainer(ctx, id); err != nil {
			b.failUnit(lb)

			return
		}

		if op == opStop {
			b.markUnitStopped(lb)

			return
		}
	}

	if err := rt.StartContainer(ctx, id); err != nil {
		b.failUnit(lb)

		return
	}

	b.awaitAndMark(lb)
}

func (b *InMemoryBackend) markUnitStopped(lb *liveCluster) {
	b.mu.Lock("redshiftEngineStopped")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return
	}

	if c, ok := b.clusters.Get(lb.id); ok && c.Status == clusterStatusPausing {
		c.Status = clusterStatusPaused
	}
}

// applyMasterPassword changes the master password inside the running engine; a no-op when none backs the cluster.
func (b *InMemoryBackend) applyMasterPassword(id, newPassword string) error {
	if newPassword == "" {
		return nil
	}

	b.mu.RLock("redshiftEnginePassword")

	lb := b.unitLocked(id)
	if lb == nil || lb.password == newPassword {
		b.mu.RUnlock()

		return nil
	}

	change := PasswordChange{Login: b.engine.login(lb), NewPassword: newPassword}
	ready, set := lb.ready, b.engine.cfg.SetPassword
	b.mu.RUnlock()

	if !ready {
		return fmt.Errorf("%w: cluster %s is not available to change its master password", ErrClusterInvalidState, id)
	}

	ctx, cancel := context.WithTimeout(lb.ctx, engineOpTimeout)
	defer cancel()

	if err := set(ctx, change); err != nil {
		logger.Load(ctx).WarnContext(ctx, "redshift: applying master password failed", "error", err)

		return fmt.Errorf("%w: the master password could not be applied to cluster %s", ErrClusterInvalidState, id)
	}

	b.mu.Lock("redshiftEnginePasswordSet")
	lb.password = newPassword
	b.mu.Unlock()

	return nil
}

// EngineLogin returns the master login of id's running engine; ok is false without a ready engine.
func (b *InMemoryBackend) EngineLogin(id string) (EngineLogin, bool) {
	b.mu.RLock("EngineLogin")
	defer b.mu.RUnlock()

	lb := b.unitLocked(id)
	if lb == nil || !lb.ready {
		return EngineLogin{}, false
	}

	return b.engine.login(lb), true
}

// EngineLogin returns the master login of the engine behind cluster id in region.
func (h *Handler) EngineLogin(region, id string) (EngineLogin, bool) {
	if bk, ok := h.BackendFor(region).(*InMemoryBackend); ok {
		return bk.EngineLogin(id)
	}

	return EngineLogin{}, false
}

// relaunchClustersLocked starts an empty container per restored cluster; passwords are not persisted, so
// each relaunched engine gets a random one until ModifyCluster sets a new one.
func (b *InMemoryBackend) relaunchClustersLocked() {
	if b.engine == nil {
		return
	}

	for _, c := range b.clusters.All() {
		if c.Status != clusterStatusDeleting {
			b.provisionClusterLocked(c, "")
		}
	}

	b.relaunchWorkgroupsLocked()
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

	if c, ok := e.cfg.Runtime.(io.Closer); ok && !e.shared {
		_ = c.Close()
	}
}
