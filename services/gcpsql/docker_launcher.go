package gcpsql

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	dumpMount      = "/gopherstack-dump"
	pgDataDir      = "/var/lib/postgresql/data"
	volumeLabel    = "gopherstack.gcpsql"
	staleVolumeAge = 10 * time.Minute
	dumpDirLabel   = "gopherstack.gcpsql.dumpdir"
	shapeLabel     = "gopherstack.gcpsql.shape"
	adoptReadyWait = 30 * time.Second
	pgPort         = "5432"
	readyInterval  = 500 * time.Millisecond
	teardownLimit  = 30 * time.Second
	maxDumpBytes   = 256 << 20
)

type containerRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

type dockerLauncher struct {
	lr      *gcpserve.LazyRuntime
	pool    *portalloc.Allocator
	log     *slog.Logger
	startup time.Duration
}

func newDockerLauncher(log *slog.Logger, pool *portalloc.Allocator, startup time.Duration) *dockerLauncher {
	return &dockerLauncher{lr: gcpserve.NewLazyRuntime(log, "gcpsql.runtime"), pool: pool, log: log, startup: startup}
}

// Close releases the runtime client.
func (l *dockerLauncher) Close() { _ = l.lr.Close() }

// Launch starts a database container and waits until it accepts authenticated connections.
func (l *dockerLauncher) Launch(ctx context.Context, spec LaunchSpec) (Engine, error) {
	r, err := l.lr.Get()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRuntimeUnavailable, err)
	}

	rt, ok := r.(containerRuntime)
	if !ok {
		return nil, fmt.Errorf("%w: runtime cannot exec or stop containers", ErrRuntimeUnavailable)
	}

	kind, image, err := parseVersion(spec.Version)
	if err != nil {
		return nil, err
	}

	if kind == kindMySQL {
		return l.launchMySQL(ctx, r, rt, spec, image)
	}

	dir, err := os.MkdirTemp("", "gopherstack-gcpsql-")
	if err != nil {
		return nil, fmt.Errorf("dump dir: %w", err)
	}

	e := &dockerEngine{
		rt: rt, pool: l.pool, image: image, id: spec.ID, password: spec.Password, dir: dir,
		mu: lockmetrics.New("gcpsql.engine"), spec: spec,
	}

	if vr, vok := r.(container.VolumeRuntime); vok && spec.Persist {
		e.vols = vr
		e.volData = volumeName("data", spec.Project, spec.ID)

		if spec.Replica == nil {
			e.volPITR = volumeName("pitr", spec.Project, spec.ID)
		}
	}

	lctx, cancel := context.WithTimeout(ctx, l.startup)
	defer cancel()

	if err = e.boot(lctx); err != nil {
		e.Close(context.WithoutCancel(ctx))

		return nil, err
	}

	return e, nil
}

func (l *dockerLauncher) launchMySQL(
	ctx context.Context, r any, rt containerRuntime, spec LaunchSpec, image string,
) (Engine, error) {
	var vr container.VolumeRuntime
	if v, ok := r.(container.VolumeRuntime); ok {
		vr = v
	}

	eng, err := mysqlengine.New(mysqlengine.Config{
		Runtime: rt, Volumes: vr, Ports: l.pool, Logger: l.log, Prefix: "gopherstack-gcpsql",
		Service: "gcpsql", ReplUser: replUser, StartTimeout: l.startup,
	})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRuntimeUnavailable, err)
	}

	srv, err := eng.Launch(ctx, mysqlSpec(spec, image))
	if err != nil {
		return nil, err //nolint:wrapcheck // launch errors carry the image
	}

	return &mysqlEngine{srv: srv}, nil
}

// SweepVolumes deletes the unused persistent volumes of instances not in keep, left by an earlier run.
func (l *dockerLauncher) SweepVolumes(ctx context.Context, keep map[string]bool) {
	r, err := l.lr.Get()
	if err != nil {
		return
	}

	vr, ok := r.(container.VolumeRuntime)
	if !ok {
		return
	}

	names, err := vr.ListUnusedVolumes(ctx, volumeLabel, staleVolumeAge)
	if err != nil {
		return
	}

	for _, n := range names {
		if !keep[n] && strings.HasPrefix(n, volumePrefix) {
			if err = vr.RemoveVolume(ctx, n); err != nil {
				logger.Load(ctx).WarnContext(ctx, "gcpsql: stale volume removal failed", "volume", n, "error", err)
			}
		}
	}
}

type dockerEngine struct {
	rt       containerRuntime
	vols     container.VolumeRuntime
	pool     *portalloc.Allocator
	mu       *lockmetrics.RWMutex
	net      *sourceNet
	ctrID    string
	image    string
	id       string
	replPass string
	dir      string
	password string
	ctrName  string
	volData  string
	volPITR  string
	netID    string
	spec     LaunchSpec
	port     int
	promoted bool
}

func (e *dockerEngine) boot(ctx context.Context) error {
	if err := e.createVolumes(ctx); err != nil {
		return err
	}

	if !e.adopt(ctx) {
		id, err := container.CreateAndStartRetry(ctx, e.rt, e.prepare, e.releasePort)
		if err != nil {
			return fmt.Errorf("start %s: %w", e.image, err)
		}

		e.setContainer(id)
	}

	if err := e.waitReady(ctx); err != nil {
		return err
	}

	if e.spec.Replica != nil {
		return nil
	}

	if e.spec.Recovery != nil {
		if err := e.waitPromoted(ctx); err != nil {
			return err
		}
	}

	return e.bootstrap(ctx)
}

func (e *dockerEngine) volumeLabels() map[string]string {
	return map[string]string{volumeLabel: "true", volumeLabel + ".instance": e.spec.Project + "/" + e.id}
}

func (e *dockerEngine) createVolumes(ctx context.Context) error {
	for _, v := range []string{e.volData, e.volPITR} {
		if v == "" {
			continue
		}

		if e.spec.Fresh {
			if err := e.vols.RemoveVolume(ctx, v); err != nil {
				return fmt.Errorf("discard stale volume %s: %w", v, err)
			}
		}

		if err := e.vols.CreateVolume(ctx, v, e.volumeLabels()); err != nil {
			return fmt.Errorf("create volume %s: %w", v, err)
		}
	}

	return nil
}

func (e *dockerEngine) setContainer(id string) {
	e.mu.Lock("setContainer")
	e.ctrID = id
	e.mu.Unlock()
}

func (e *dockerEngine) container() string {
	e.mu.RLock("container")
	defer e.mu.RUnlock()

	return e.ctrID
}

func (e *dockerEngine) pass() string {
	e.mu.RLock("pass")
	defer e.mu.RUnlock()

	return e.password
}

func (e *dockerEngine) prepare() (container.Spec, error) {
	port, err := portalloc.AcquireFrom(e.pool, "gcpsql")
	if err != nil {
		return container.Spec{}, fmt.Errorf("host port: %w", err)
	}

	e.mu.Lock("prepare")
	e.port = port
	e.mu.Unlock()

	name := "gopherstack-gcpsql-" + sanitizeName(e.id) + "-" + strconv.Itoa(port)

	e.mu.Lock("prepare")
	e.ctrName = name
	e.mu.Unlock()

	spec := container.Spec{
		Name:        name,
		Labels:      e.ownerLabels(),
		Image:       e.image,
		Mounts:      []string{e.dir + ":" + dumpMount},
		MemoryBytes: e.spec.MemoryBytes,
		NanoCPUs:    e.spec.NanoCPUs,
	}

	if r := e.netSource(); r != nil {
		spec.Network = r.Network

		e.mu.Lock("prepare")
		e.netID = r.NetworkID
		e.mu.Unlock()
	}

	spec.Env = []string{"POSTGRES_USER=" + pgAdminUser, "POSTGRES_PASSWORD=" + e.pass()}
	e.pgLaunch(&spec)

	spec.Ports = []string{container.PortSpec(container.LoopbackHost, strconv.Itoa(port), pgPort)}

	return spec, nil
}

func (e *dockerEngine) releasePort() {
	e.mu.Lock("releasePort")
	port := e.port
	e.port = 0
	e.mu.Unlock()

	portalloc.ReleaseTo(e.pool, port)
}

func sanitizeName(s string) string {
	b := []byte(s)
	for i, c := range b {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			b[i] = '-'
		}
	}

	return string(b)
}

// Endpoint implements Engine.
func (e *dockerEngine) Endpoint() (string, int) {
	e.mu.RLock("Endpoint")
	defer e.mu.RUnlock()

	return container.LoopbackHost, e.port
}

func (e *dockerEngine) waitReady(ctx context.Context) error {
	tick := time.NewTicker(readyInterval)
	defer tick.Stop()

	for {
		if e.ping(ctx) == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("database did not accept connections: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func (e *dockerEngine) ping(ctx context.Context) error {
	db, err := e.open(e.adminDB())
	if err != nil {
		return err
	}
	defer db.Close()

	pctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	return db.PingContext(pctx)
}

func (e *dockerEngine) adminDB() string { return pgAdminUser }

// Stop implements Engine.
func (e *dockerEngine) Stop(ctx context.Context) error {
	return e.rt.StopContainer(ctx, e.container())
}

// Start implements Engine.
func (e *dockerEngine) Start(ctx context.Context) error {
	if err := e.rt.StartContainer(ctx, e.container()); err != nil {
		return err //nolint:wrapcheck // runtime errors already carry the container id
	}

	return e.waitReady(ctx)
}

// Close implements Engine; it removes the container, its port and the dump directory.
func (e *dockerEngine) Close(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownLimit)
	defer cancel()

	if id := e.container(); id != "" {
		if err := e.rt.StopAndRemove(ctx, id); err != nil {
			logger.Load(ctx).WarnContext(ctx, "gcpsql: container removal failed", "container", id, "error", err)
		}
	}

	e.releaseNetwork(ctx)
	e.releasePort()

	_ = os.RemoveAll(e.dir)
}
