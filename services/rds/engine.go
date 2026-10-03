package rds

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

// Engine modes for RDS.
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

	engineStartFailedCode = "DB_ENGINE_START_FAILED"
	engineRandPassBytes   = 12

	instanceStatusFailed   = "failed"
	instanceStatusStarting = "starting"
	instanceStatusStopping = "stopping"

	unitInstancePrefix = "i:"
	unitClusterPrefix  = "c:"

	postgresScheme = "postgres"
	mysqlRootUser  = "root"

	pgContainerPort    = 5432
	mysqlContainerPort = 3306

	versionSegmentsMajor      = 1
	versionSegmentsMajorMinor = 2

	defaultPostgresMajor = "17"
	defaultMySQLVersion  = "8.4"
	defaultMariaVersion  = "11.4"
)

var engineUserRegex = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{0,15}$`)

// EngineRuntime is the slice of the container runtime the database engine needs.
type EngineRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
}

// EngineLogin identifies a database login on a running engine.
type EngineLogin struct {
	Kind     string
	Addr     string
	User     string
	Password string
	DBName   string
}

// EngineConfig configures docker-backed RDS databases.
type EngineConfig struct {
	Runtime EngineRuntime
	// Ports hands out host ports; nil falls back to OS-chosen free ports.
	Ports *portalloc.Allocator
	// Probe returns nil once the login works against the engine; nil uses the real drivers.
	Probe func(ctx context.Context, l EngineLogin) error
	// SetPassword changes the login's own password inside the engine; nil uses the real drivers.
	SetPassword func(ctx context.Context, l EngineLogin, newPassword string) error
	// Host is the address clients use to reach published ports; default 127.0.0.1.
	Host string
	// StartTimeout bounds how long a database may stay unreachable before it is marked failed.
	StartTimeout time.Duration
}

type liveDB struct {
	gone        chan struct{}
	key         string
	resourceID  string
	kind        string
	version     string
	user        string
	password    string
	dbName      string
	containerID string
	port        int
	once        sync.Once
	ready       bool
}

func (lb *liveDB) release() { lb.once.Do(func() { close(lb.gone) }) }

// opContext returns a context cancelled when timeout elapses (if > 0) or the unit is dropped.
func (lb *liveDB) opContext(timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
	}

	go func() {
		select {
		case <-lb.gone:
			cancel()
		case <-ctx.Done():
		}
	}()

	return ctx, cancel
}

type dbEngine struct {
	units map[string]*liveDB
	cfg   EngineConfig
	wg    sync.WaitGroup
}

// EnableEngine switches the backend to docker mode: postgres/mysql/mariadb (and Aurora) get a real container.
func (b *InMemoryBackend) EnableEngine(cfg EngineConfig) {
	if cfg.Host == "" {
		cfg.Host = engineDefaultHost
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = engineStartTimeout
	}

	if cfg.Probe == nil {
		cfg.Probe = probeEngine
	}

	if cfg.SetPassword == nil {
		cfg.SetPassword = setEnginePassword
	}

	b.mu.Lock("EnableEngine")
	defer b.mu.Unlock()

	b.engine = &dbEngine{cfg: cfg, units: make(map[string]*liveDB)}
}

// engineKind maps an RDS engine name to its base database; "" means no real engine is available.
func engineKind(engine string) string {
	switch engine {
	case enginePostgres, engineAuroraPostgresql:
		return enginePostgres
	case engineMySQL, engineAuroraMySQL:
		return engineMySQL
	case engineMariaDB:
		return engineMariaDB
	default:
		return ""
	}
}

func engineImage(kind, version string) string {
	switch kind {
	case enginePostgres:
		return "postgres:" + pickVersion(
			version, defaultPostgresMajor, versionSegmentsMajor, "13", "14", "15", "16", "17")
	case engineMySQL:
		return "mysql:" + pickVersion(version, defaultMySQLVersion, versionSegmentsMajorMinor, "8.0", "8.4")
	default:
		return "mariadb:" + pickVersion(
			version, defaultMariaVersion, versionSegmentsMajorMinor, "10.6", "10.11", "11.4", "11.8")
	}
}

// pickVersion returns the first n dot-segments of version when listed in allowed, else the default.
func pickVersion(version, fallback string, segments int, allowed ...string) string {
	parts := strings.Split(version, ".")
	if len(parts) < segments {
		return fallback
	}

	prefix := strings.Join(parts[:segments], ".")
	if slices.Contains(allowed, prefix) {
		return prefix
	}

	return fallback
}

func engineContainerPort(kind string) int {
	if kind == enginePostgres {
		return pgContainerPort
	}

	return mysqlContainerPort
}

func engineEnv(lb *liveDB) []string {
	if lb.kind == enginePostgres {
		env := []string{"POSTGRES_USER=" + lb.user, "POSTGRES_PASSWORD=" + lb.password}
		if lb.dbName != "" {
			env = append(env, "POSTGRES_DB="+lb.dbName)
		}

		return env
	}

	env := []string{"MYSQL_ROOT_PASSWORD=" + lb.password}
	if lb.user != mysqlRootUser {
		env = append(env, "MYSQL_USER="+lb.user, "MYSQL_PASSWORD="+lb.password)
	}

	if lb.dbName != "" {
		env = append(env, "MYSQL_DATABASE="+lb.dbName)
	}

	return env
}

func (e *dbEngine) spec(lb *liveDB) container.Spec {
	return container.Spec{
		Name:  "gopherstack-rds-" + uuid.NewString()[:12],
		Image: engineImage(lb.kind, lb.version),
		Env:   engineEnv(lb),
		Ports: []string{container.PortSpec(
			container.BindHostFor(e.cfg.Host), strconv.Itoa(lb.port), strconv.Itoa(engineContainerPort(lb.kind)))},
	}
}

func (e *dbEngine) login(lb *liveDB) EngineLogin {
	return EngineLogin{
		Kind:     lb.kind,
		Addr:     net.JoinHostPort(e.cfg.Host, strconv.Itoa(lb.port)),
		User:     lb.user,
		Password: lb.password,
		DBName:   lb.dbName,
	}
}

func (e *dbEngine) acquirePort() (int, error) {
	if e.cfg.Ports != nil {
		return e.cfg.Ports.Acquire("rds") //nolint:wrapcheck // allocator error is self-describing
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
			logger.Load(ctx).WarnContext(ctx, "rds: engine container removal failed",
				"container", containerID, "error", err)
		}
	}

	e.releasePort(port)
}

// reap cancels the units' work and removes their containers in the background.
func (e *dbEngine) reap(lbs ...*liveDB) {
	for _, lb := range lbs {
		if lb == nil {
			continue
		}

		lb.release()

		id, port := lb.containerID, lb.port
		e.wg.Go(func() { e.remove(id, port) })
	}
}

func randomPassword() string {
	buf := make([]byte, engineRandPassBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

// validateEngineLogin rejects master usernames the real engines cannot be provisioned with.
func (b *InMemoryBackend) validateEngineLogin(engine, masterUser string) error {
	if b.engine == nil || engineKind(engine) == "" || engineUserRegex.MatchString(masterUser) {
		return nil
	}

	return fmt.Errorf("%w: docker engine requires a MasterUsername of 1-16 letters, digits or underscores "+
		"starting with a letter", ErrInvalidParameter)
}

func unitKeyForInstance(id string) string { return unitInstancePrefix + normalizeID(id) }
func unitKeyForCluster(id string) string  { return unitClusterPrefix + normalizeID(id) }

// instanceUnitLocked returns the live database serving an instance (its cluster's, else its own).
func (b *InMemoryBackend) instanceUnitLocked(inst *DBInstance) *liveDB {
	if b.engine == nil {
		return nil
	}

	if inst.DBClusterIdentifier != "" {
		if lb, ok := b.engine.units[unitKeyForCluster(inst.DBClusterIdentifier)]; ok {
			return lb
		}
	}

	return b.engine.units[unitKeyForInstance(inst.DBInstanceIdentifier)]
}

func (b *InMemoryBackend) clusterUnitLocked(id string) *liveDB {
	if b.engine == nil {
		return nil
	}

	return b.engine.units[unitKeyForCluster(id)]
}

func (b *InMemoryBackend) dropUnitLocked(key string) {
	if b.engine == nil {
		return
	}

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

// launchUnitLocked registers a database unit and starts its container in the background.
func (b *InMemoryBackend) launchUnitLocked(key, resourceID, engine, version, user, password, dbName string) {
	e := b.engine
	if e == nil || b.closed {
		return
	}

	if password == "" {
		password = randomPassword()
	}

	lb := &liveDB{
		gone: make(chan struct{}), key: key, resourceID: resourceID, kind: engineKind(engine),
		version: version, user: user, password: password, dbName: dbName,
	}
	e.units[key] = lb

	e.wg.Go(func() { b.runUnit(lb) })
}

// provisionInstanceLocked starts (or joins) the real engine for a new instance; true means the engine
// drives the instance's status to available instead of the timer.
func (b *InMemoryBackend) provisionInstanceLocked(inst *DBInstance, password string) bool {
	if b.engine == nil || engineKind(inst.Engine) == "" {
		return false
	}

	if inst.DBClusterIdentifier != "" {
		lb := b.clusterUnitLocked(inst.DBClusterIdentifier)
		if lb == nil {
			return false
		}

		if lb.ready {
			inst.Endpoint, inst.Port = b.engine.cfg.Host, lb.port

			return false
		}

		return true
	}

	b.launchUnitLocked(unitKeyForInstance(inst.DBInstanceIdentifier), inst.DBInstanceIdentifier,
		inst.Engine, inst.EngineVersion, inst.MasterUsername, password, inst.DBName)

	return true
}

// provisionClusterLocked starts the single real engine backing a cluster; the cluster stays creating until it is up.
func (b *InMemoryBackend) provisionClusterLocked(c *DBCluster, password string) {
	if b.engine == nil || engineKind(c.Engine) == "" {
		return
	}

	c.Status = instanceStatusCreating
	b.launchUnitLocked(unitKeyForCluster(c.DBClusterIdentifier), c.DBClusterIdentifier,
		c.Engine, c.EngineVersion, c.MasterUsername, password, c.DatabaseName)
}

func (b *InMemoryBackend) runUnit(lb *liveDB) {
	e := b.engine
	ctx, cancel := lb.opContext(0)

	defer cancel()

	port, err := e.acquirePort()
	if err != nil {
		b.failUnit(ctx, lb)

		return
	}

	if !b.recordPort(lb, port) {
		e.releasePort(port)

		return
	}

	id, err := e.cfg.Runtime.CreateAndStart(ctx, e.spec(lb))
	if err != nil {
		b.failUnit(ctx, lb)

		return
	}

	if !b.recordContainer(lb, id) {
		e.remove(id, port)

		return
	}

	b.awaitAndMark(ctx, lb)
}

func (b *InMemoryBackend) attachedLocked(lb *liveDB) bool {
	return b.engine != nil && b.engine.units[lb.key] == lb
}

func (b *InMemoryBackend) recordPort(lb *liveDB, port int) bool {
	b.mu.Lock("rdsEnginePort")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.port = port

	return true
}

func (b *InMemoryBackend) recordContainer(lb *liveDB, id string) bool {
	b.mu.Lock("rdsEngineContainer")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.containerID = id

	return true
}

func (b *InMemoryBackend) awaitAndMark(ctx context.Context, lb *liveDB) {
	b.mu.RLock("rdsEngineLogin")
	login := b.engine.login(lb)
	b.mu.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, b.engine.cfg.StartTimeout)
	defer cancel()

	if b.engine.awaitReady(ctx, login) != nil {
		b.failUnit(ctx, lb)

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

// unitStatusesLocked applies fn to the instance and cluster records a unit backs.
func (b *InMemoryBackend) unitStatusesLocked(lb *liveDB, onInstance func(*DBInstance), onCluster func(*DBCluster)) {
	if strings.HasPrefix(lb.key, unitInstancePrefix) {
		if inst, ok := b.instances.Get(normalizeID(lb.resourceID)); ok {
			onInstance(inst)
		}

		return
	}

	if c, ok := b.clusters.Get(normalizeID(lb.resourceID)); ok {
		onCluster(c)
	}

	for _, inst := range b.instances.All() {
		if idEqual(inst.DBClusterIdentifier, lb.resourceID) {
			onInstance(inst)
		}
	}
}

func transitional(status string) bool {
	switch status {
	case instanceStatusCreating, instanceStatusStarting, instanceStatusRebooting:
		return true
	default:
		return false
	}
}

func (b *InMemoryBackend) markUnitReady(lb *liveDB) {
	b.mu.Lock("rdsEngineReady")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return
	}

	lb.ready = true
	host := b.engine.cfg.Host

	b.unitStatusesLocked(lb, func(inst *DBInstance) {
		inst.Endpoint, inst.Port = host, lb.port

		if transitional(inst.DBInstanceStatus) {
			inst.DBInstanceStatus = instanceStatusAvailable
			delete(b.instanceReadyAt, inst.DBInstanceIdentifier)
			b.publishInstanceEventLocked(inst.DBInstanceIdentifier, "DB instance is now available")
		}
	}, func(c *DBCluster) {
		c.Endpoint, c.ReaderEndpoint, c.Port = host, host, lb.port

		if transitional(c.Status) {
			c.Status = instanceStatusAvailable
			delete(b.clusterReadyAt, c.DBClusterIdentifier)
			b.publishClusterEventLocked(c.DBClusterIdentifier, "DB cluster is now available")
		}
	})
}

// failUnit marks a still-wanted unit's resources failed and releases its container.
func (b *InMemoryBackend) failUnit(ctx context.Context, lb *liveDB) {
	b.mu.Lock("rdsEngineFail")

	if !b.attachedLocked(lb) {
		b.mu.Unlock()

		return
	}

	delete(b.engine.units, lb.key)
	b.unitStatusesLocked(lb, func(inst *DBInstance) {
		inst.DBInstanceStatus = instanceStatusFailed
		delete(b.instanceReadyAt, inst.DBInstanceIdentifier)
	}, func(c *DBCluster) {
		c.Status = instanceStatusFailed
		delete(b.clusterReadyAt, c.DBClusterIdentifier)
	})
	b.mu.Unlock()

	logger.Load(ctx).WarnContext(ctx, "rds: database engine failed to start", "code", engineStartFailedCode)
	b.engine.reap(lb)
}

// Close removes every database container and waits for in-flight work to finish.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")

	if b.closed {
		b.mu.Unlock()

		return
	}

	b.closed = true
	close(b.stopCh)
	b.dropAllUnitsLocked()
	e := b.engine
	b.mu.Unlock()

	b.reconcilerWG.Wait()

	if e == nil {
		return
	}

	e.wg.Wait()

	if c, ok := e.cfg.Runtime.(io.Closer); ok {
		_ = c.Close()
	}
}

// Shutdown removes every database container; a no-op when already closed.
func (h *Handler) Shutdown(_ context.Context) {
	h.Backend.Close()
}
