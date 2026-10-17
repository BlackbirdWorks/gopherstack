package docdb

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
	Runtime EngineRuntime
	// Ports hands out host ports; nil falls back to OS-chosen free ports.
	Ports *portalloc.Allocator
	// Probe returns nil once the MongoDB server at addr answers; nil uses the OP_MSG wire probe.
	Probe func(ctx context.Context, addr string) error
	// SetPassword changes the master password inside the engine; nil execs the mongo shell in the container.
	SetPassword func(ctx context.Context, change PasswordChange) error
	// Host is the address clients use to reach published ports; default 127.0.0.1.
	Host string
	// StartTimeout bounds how long a cluster may stay unreachable before it is marked failed.
	StartTimeout time.Duration
}

type liveCluster struct {
	ctx         context.Context
	cancel      context.CancelFunc
	key         string
	region      string
	id          string
	version     string
	user        string
	password    string
	containerID string
	port        int
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

// EnableEngine switches the backend to docker mode: each cluster gets a real MongoDB container.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) {
	if cfg.Host == "" {
		cfg.Host = engineDefaultHost
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = engineStartTimeout
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

	b.engine = &dbEngine{cfg: cfg, units: make(map[string]*liveCluster)}
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
	return container.Spec{
		Name:  "gopherstack-docdb-" + uuid.NewString()[:12],
		Image: "mongo:" + lb.version,
		Env:   []string{"MONGO_INITDB_ROOT_USERNAME=" + lb.user, "MONGO_INITDB_ROOT_PASSWORD=" + lb.password},
		Ports: []string{container.PortSpec(
			container.BindHostFor(e.cfg.Host), strconv.Itoa(lb.port), strconv.Itoa(mongoContainerPort))},
	}
}

func (e *dbEngine) addr(lb *liveCluster) string {
	return net.JoinHostPort(e.cfg.Host, strconv.Itoa(lb.port))
}

func (e *dbEngine) acquirePort() (int, error) {
	if e.cfg.Ports != nil {
		return e.cfg.Ports.Acquire("docdb") //nolint:wrapcheck // allocator error is self-describing
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
			logger.Load(ctx).WarnContext(ctx, "docdb: engine container removal failed",
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
	}
	e.units[lb.key] = lb
	c.Status = statusCreating
	c.readyAt = time.Time{}

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
	b.mu.Lock("docdbEnginePort")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.port = port

	return true
}

func (b *InMemoryBackend) recordContainer(lb *liveCluster, id string) bool {
	b.mu.Lock("docdbEngineContainer")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.containerID = id

	return true
}

func (b *InMemoryBackend) awaitAndMark(lb *liveCluster) {
	b.mu.RLock("docdbEngineAddr")
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

func execSetPassword(ctx context.Context, rt EngineRuntime, c PasswordChange) error {
	user, pass := jsString(c.User), jsString(c.NewPassword)
	script := "db.getSiblingDB('admin').changeUserPassword(" + user + "," + pass + ")"
	cmd := []string{
		mongoShell, "--quiet", "-u", c.User, "-p", c.OldPassword,
		"--authenticationDatabase", "admin", "--eval", script,
	}

	res, err := rt.Exec(ctx, c.ContainerID, cmd)
	if err != nil {
		return fmt.Errorf("exec mongo shell: %w", err)
	}

	if res.ExitCode != 0 {
		return fmt.Errorf("%w: exit %d: %s", errShellFailed, res.ExitCode, strings.TrimSpace(res.Stderr))
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
