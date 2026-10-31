package rds

import (
	"cmp"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

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
	// AdminPassword is the sa password of a SQL Server container; empty for other engines.
	AdminPassword string
}

// EngineConfig configures docker-backed RDS databases.
type EngineConfig struct {
	Runtime       EngineRuntime
	Ports         *portalloc.Allocator
	Probe         func(ctx context.Context, l EngineLogin) error
	SetPassword   func(ctx context.Context, l EngineLogin, newPassword string) error
	Now           func() time.Time
	ReplicaStatus func(ctx context.Context, l EngineLogin) (ReplicaStatus, error)
	// RoleLookup reports whether the unit key has an IAM role associated for a feature.
	RoleLookup func(key, feature string) bool
	Host       string
	CADir      string
	DumpDir    string
	// APIEndpoint is the emulator's own base URL, used by the aws_s3/aws_lambda callouts.
	APIEndpoint string
	// HostArch overrides the CPU architecture checked before launching SQL Server; empty means runtime.GOARCH.
	HostArch             string
	ReplicaPollInterval  time.Duration
	StartTimeout         time.Duration
	RestorePointInterval time.Duration
	// PostgresExtensions runs postgres units on an image with postgis and pgvector plus the aws_s3/aws_lambda extensions.
	PostgresExtensions bool
	// SQLServer runs sqlserver-* instances on a real SQL Server container (amd64 only, opt-in).
	SQLServer bool
}

type liveDB struct {
	gone        chan struct{}
	capture     chan struct{}
	proxy       atomic.Pointer[pgProxy]
	fwd         atomic.Pointer[tcpForwarder]
	net         atomic.Pointer[replNet]
	extDir      atomic.Pointer[string]
	repl        *replicaState
	eps         sync.Map
	kind        string
	containerID string
	replPass    string
	ctrName     string
	key         string
	version     string
	engine      string
	user        string
	resourceID  string
	dbName      string
	password    string
	seed        engineSeed
	port        int
	once        sync.Once
	serverID    uint32
	monitoring  atomic.Bool
	dbPort      atomic.Int32
	ready       bool
	paused      bool
}

func (lb *liveDB) release() {
	lb.once.Do(func() {
		close(lb.gone)

		if p := lb.proxy.Load(); p != nil {
			p.close()
		}

		if f := lb.fwd.Load(); f != nil {
			f.close()
		}

		lb.eps.Range(func(_, v any) bool {
			if l, ok := v.(*endpointListener); ok {
				l.close()
			}

			return true
		})
	})
}

// observable reports whether client connections pass through a proxy that counts them.
func (lb *liveDB) observable() bool { return lb.proxy.Load() != nil || lb.fwd.Load() != nil }

// advertisedPort is the port clients connect to: the IAM-aware proxy for postgres, else the database itself.
func (lb *liveDB) advertisedPort() int {
	if p := lb.proxy.Load(); p != nil {
		return p.port
	}

	if f := lb.fwd.Load(); f != nil {
		return f.port
	}

	return lb.port
}

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
	units      map[string]*liveDB
	data       map[string]engineData
	points     map[string][]restorePoint
	checked    map[string]time.Time
	failures   map[string]string
	store      *dumpStore
	ca         *engineCA
	tlsCfg     *tls.Config
	ext        *pgExt
	callout    *calloutServer
	cfg        EngineConfig
	wg         sync.WaitGroup
	tlsOnce    sync.Once
	calloutMu  sync.Mutex
	execBroken atomic.Bool
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

	if cfg.ReplicaStatus == nil {
		cfg.ReplicaStatus = queryReplicaStatus
	}

	if cfg.SetPassword == nil {
		cfg.SetPassword = setEnginePassword
	}

	b.mu.Lock("EnableEngine")
	defer b.mu.Unlock()

	if cfg.Now == nil {
		cfg.Now = time.Now
	}

	b.engine = &dbEngine{
		cfg: cfg, units: make(map[string]*liveDB), data: make(map[string]engineData),
		points: make(map[string][]restorePoint), checked: make(map[string]time.Time),
		ext: newPgExt(),
	}
	if cfg.PostgresExtensions && cfg.RoleLookup == nil {
		b.engine.cfg.RoleLookup = b.unitHasRole
	}

	if cfg.DumpDir != "" {
		b.engine.store = newDumpStore(filepath.Join(cfg.DumpDir, b.region))
		b.engine.loadDumps()
	}

	b.startRestorePointLoop()
}

// kindOf is engineKind plus the opt-in SQL Server editions; a nil engine knows no kind.
func (e *dbEngine) kindOf(engine string) string {
	if k := engineKind(engine); k != "" {
		return k
	}

	if e != nil && e.cfg.SQLServer && sqlServerEngine(engine) {
		return engineSQLServer
	}

	return ""
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
	case engineSQLServer:
		return sqlServerImage(version)
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
	switch kind {
	case enginePostgres:
		return pgContainerPort
	case engineSQLServer:
		return sqlServerContainerPort
	}

	return mysqlContainerPort
}

func engineEnv(lb *liveDB) []string {
	if lb.kind == engineSQLServer {
		return sqlServerEnv(lb, lb.engine)
	}

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

func (e *dbEngine) spec(lb *liveDB, plan replPlan) container.Spec {
	s := container.Spec{
		Name:   lb.ctrName,
		Labels: container.ServiceLabels("rds"),
		Image:  engineImage(lb.kind, lb.version),
		Env:    engineEnv(lb),
		Ports: []string{container.PortSpec(
			container.BindHostFor(e.cfg.Host), strconv.Itoa(lb.port), strconv.Itoa(engineContainerPort(lb.kind)))},
		Network: plan.network,
	}
	if lb.kind != engineSQLServer {
		applyReplicationFlags(&s, lb, plan)
	}

	e.applyPgExt(&s, lb)

	return s
}

func (e *dbEngine) login(lb *liveDB) EngineLogin {
	l := EngineLogin{
		Kind:     lb.kind,
		Addr:     net.JoinHostPort(e.cfg.Host, strconv.Itoa(lb.port)),
		User:     lb.user,
		Password: lb.password,
		DBName:   lb.dbName,
	}

	if lb.kind == engineSQLServer {
		l.AdminPassword = sqlServerSAPassword(lb.ctrName)
	}

	return l
}

func (e *dbEngine) acquirePort() (int, error) {
	return portalloc.AcquireFrom(e.cfg.Ports, "rds")
}

func (e *dbEngine) releasePort(port int) {
	portalloc.ReleaseTo(e.cfg.Ports, port)
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
		netw := lb.net.Load()
		e.wg.Go(func() {
			if p := lb.proxy.Load(); p != nil {
				p.wg.Wait()
			}

			if f := lb.fwd.Load(); f != nil {
				f.wg.Wait()
			}

			lb.eps.Range(func(_, v any) bool {
				if l, ok := v.(*endpointListener); ok {
					l.wg.Wait()
				}

				return true
			})

			e.remove(id, port)
			e.releaseNet(netw)
		})
	}
}

func randomPassword() string {
	buf := make([]byte, engineRandPassBytes)
	_, _ = rand.Read(buf)

	return hex.EncodeToString(buf)
}

// validateEngineLogin rejects master usernames the real engines cannot be provisioned with.
func (b *InMemoryBackend) validateEngineLogin(engine, masterUser string) error {
	if b.engine == nil || b.engine.kindOf(engine) == "" || engineUserRegex.MatchString(masterUser) {
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

	b.dropPointsLocked(key)

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

	if b.engine.store == nil {
		clear(b.engine.data)
		clear(b.engine.points)
		clear(b.engine.checked)
	}
}

// launchSeededUnitLocked registers a database unit and starts its container in the background; the seed is
// loaded once the engine first comes up.
func (b *InMemoryBackend) launchSeededUnitLocked(
	key, resourceID, engine, version, user, password, dbName string, seed engineSeed,
) {
	e := b.engine
	if e == nil || b.closed {
		return
	}

	if password == "" {
		password = cmp.Or(seed.password, randomPassword())
	}

	lb := &liveDB{
		gone: make(chan struct{}), capture: make(chan struct{}, 1), key: key, resourceID: resourceID,
		kind: e.kindOf(engine), engine: engine,
		version: version, user: user, password: password, dbName: dbName, seed: seed,
		ctrName: "gopherstack-rds-" + randomPassword(), serverID: randomServerID(),
	}
	lb.dbPort.Store(int32(b.recordedPortLocked(lb))) //nolint:gosec // TCP port fits int32
	if seed.replicate {
		lb.repl = &replicaState{srcKey: seed.fromKey}
		lb.net.Store(seed.net)
	}

	e.units[key] = lb

	e.wg.Go(func() { b.runUnit(lb) })
}

// provisionInstanceLocked starts (or joins) the real engine for a new instance; true means the engine
// drives the instance's status to available instead of the timer.
func (b *InMemoryBackend) provisionInstanceLocked(inst *DBInstance, password string) bool {
	return b.provisionSeededInstanceLocked(inst, password, engineSeed{})
}

// provisionSeededInstanceLocked is provisionInstanceLocked for an instance restored from existing data.
func (b *InMemoryBackend) provisionSeededInstanceLocked(inst *DBInstance, password string, seed engineSeed) bool {
	if b.engine == nil || b.engine.kindOf(inst.Engine) == "" {
		return false
	}

	if inst.DBClusterIdentifier != "" {
		lb := b.clusterUnitLocked(inst.DBClusterIdentifier)
		if lb == nil {
			return false
		}

		if lb.ready {
			inst.Endpoint, inst.Port = b.engine.cfg.Host, lb.advertisedPort()

			return false
		}

		return true
	}

	b.launchSeededUnitLocked(unitKeyForInstance(inst.DBInstanceIdentifier), inst.DBInstanceIdentifier,
		inst.Engine, inst.EngineVersion, inst.MasterUsername, password, inst.DBName, seed)

	return true
}

// provisionClusterLocked starts the single real engine backing a cluster; the cluster stays creating until it is up.
func (b *InMemoryBackend) provisionClusterLocked(c *DBCluster, password string) bool {
	return b.provisionSeededClusterLocked(c, password, engineSeed{})
}

// provisionSeededClusterLocked is provisionClusterLocked for a cluster restored from existing data.
func (b *InMemoryBackend) provisionSeededClusterLocked(c *DBCluster, password string, seed engineSeed) bool {
	if b.engine == nil || engineKind(c.Engine) == "" {
		return false
	}

	c.Status = instanceStatusCreating
	b.launchSeededUnitLocked(unitKeyForCluster(c.DBClusterIdentifier), c.DBClusterIdentifier,
		c.Engine, c.EngineVersion, c.MasterUsername, password, c.DatabaseName, seed)

	return true
}

var errUnitDetached = errors.New("engine unit detached")

func (b *InMemoryBackend) runUnit(lb *liveDB) {
	e := b.engine
	ctx, cancel := lb.opContext(0)

	defer cancel()

	if lb.kind == engineSQLServer && cmp.Or(e.cfg.HostArch, runtime.GOARCH) != "amd64" {
		b.failUnit(ctx, lb, "check platform", errSQLServerArch)

		return
	}

	var port int

	id, err := container.CreateAndStartRetry(ctx, e.cfg.Runtime,
		func() (container.Spec, error) {
			p, err := e.acquirePort()
			if err != nil {
				return container.Spec{}, err
			}

			port = p

			if !b.recordPort(lb, p) {
				e.releasePort(p)

				return container.Spec{}, errUnitDetached
			}

			if perr := e.preparePgExt(ctx, lb); perr != nil {
				e.releasePort(p)

				return container.Spec{}, perr
			}

			plan, err := b.replicaPlan(ctx, lb)
			if err != nil {
				e.releasePort(p)

				return container.Spec{}, err
			}

			return e.spec(lb, plan), nil
		},
		func() {
			e.releasePort(port)
			b.recordPort(lb, 0)
		})
	if err != nil {
		if !errors.Is(err, errUnitDetached) {
			b.failUnit(ctx, lb, "create and start container", err)
		}

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

	if stage, err := b.awaitUnit(ctx, lb, login); err != nil {
		b.failUnit(ctx, lb, stage, err)

		return
	}

	if lb.kind == enginePostgres && lb.proxy.Load() == nil {
		if err := b.startProxy(lb); err != nil {
			b.failUnit(ctx, lb, "start proxy", err)

			return
		}
	}

	if lb.kind != enginePostgres && lb.fwd.Load() == nil {
		if err := b.startForwarder(lb); err != nil {
			b.failUnit(ctx, lb, "start forwarder", err)

			return
		}
	}

	b.markUnitReady(lb)
	b.startReplicaMonitor(lb)
	b.recordRestorePoint(lb)
}

func (b *InMemoryBackend) awaitUnit(ctx context.Context, lb *liveDB, login EngineLogin) (string, error) {
	if err := b.engine.awaitReady(ctx, login); err != nil {
		return "await readiness", err
	}

	if err := b.applySeed(ctx, lb); err != nil {
		return "apply seed", err
	}

	if err := b.guardReplica(ctx, lb); err != nil {
		return "guard replica", err
	}

	return "", nil
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

	defer b.syncClusterEndpointsLocked(lb)

	b.unitStatusesLocked(lb, func(inst *DBInstance) {
		inst.Endpoint, inst.Port = host, lb.advertisedPort()

		if transitional(inst.DBInstanceStatus) {
			inst.DBInstanceStatus = instanceStatusAvailable
			delete(b.instanceReadyAt, inst.DBInstanceIdentifier)
			b.publishInstanceEventLocked(inst.DBInstanceIdentifier, "DB instance is now available")
		}
	}, func(c *DBCluster) {
		c.Endpoint, c.ReaderEndpoint, c.Port = host, host, lb.advertisedPort()

		if transitional(c.Status) {
			c.Status = instanceStatusAvailable
			delete(b.clusterReadyAt, c.DBClusterIdentifier)
			b.publishClusterEventLocked(c.DBClusterIdentifier, "DB cluster is now available")
		}
	})
}

// failUnit marks a still-wanted unit's resources failed and releases its container.
func (b *InMemoryBackend) failUnit(ctx context.Context, lb *liveDB, stage string, cause error) {
	reason := b.failureReason(lb, stage, cause)

	b.mu.Lock("rdsEngineFail")

	if !b.attachedLocked(lb) {
		b.mu.Unlock()

		return
	}

	delete(b.engine.units, lb.key)

	if b.engine.failures == nil {
		b.engine.failures = make(map[string]string)
	}

	b.engine.failures[normalizeID(lb.resourceID)] = reason
	b.unitStatusesLocked(lb, func(inst *DBInstance) {
		inst.DBInstanceStatus = instanceStatusFailed
		delete(b.instanceReadyAt, inst.DBInstanceIdentifier)
	}, func(c *DBCluster) {
		c.Status = instanceStatusFailed
		delete(b.clusterReadyAt, c.DBClusterIdentifier)
	})
	b.mu.Unlock()

	logger.Load(ctx).WarnContext(ctx, "rds: database engine failed to start",
		"code", engineStartFailedCode, "resource", lb.resourceID, "reason", reason)
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
	b.pauseWG.Wait()

	if e == nil {
		return
	}

	e.wg.Wait()
	e.stopCallout()

	if c, ok := e.cfg.Runtime.(io.Closer); ok {
		_ = c.Close()
	}
}

// Shutdown removes every database container; a no-op when already closed.
func (h *Handler) Shutdown(_ context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, bk := range h.backends {
		bk.Close()
	}

	h.Backend.Close()
}
