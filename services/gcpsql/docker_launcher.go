package gcpsql

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	dumpMount     = "/gopherstack-dump"
	pgPort        = "5432"
	mysqlPort     = "3306"
	readyInterval = 500 * time.Millisecond
	teardownLimit = 30 * time.Second
	maxDumpBytes  = 256 << 20
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
	startup time.Duration
}

func newDockerLauncher(log *slog.Logger, pool *portalloc.Allocator, startup time.Duration) *dockerLauncher {
	return &dockerLauncher{lr: gcpserve.NewLazyRuntime(log, "gcpsql.runtime"), pool: pool, startup: startup}
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

	dir, err := os.MkdirTemp("", "gopherstack-gcpsql-")
	if err != nil {
		return nil, fmt.Errorf("dump dir: %w", err)
	}

	e := &dockerEngine{
		rt: rt, pool: l.pool, kind: kind, image: image, id: spec.ID, password: spec.Password, dir: dir,
		mu: lockmetrics.New("gcpsql.engine"),
	}

	lctx, cancel := context.WithTimeout(ctx, l.startup)
	defer cancel()

	if err = e.boot(lctx); err != nil {
		e.Close(context.WithoutCancel(ctx))

		return nil, err
	}

	return e, nil
}

type dockerEngine struct {
	rt       containerRuntime
	pool     *portalloc.Allocator
	mu       *lockmetrics.RWMutex
	image    string
	id       string
	password string
	dir      string
	ctrID    string
	port     int
	kind     dbKind
}

func (e *dockerEngine) boot(ctx context.Context) error {
	id, err := container.CreateAndStartRetry(ctx, e.rt, e.prepare, e.releasePort)
	if err != nil {
		return fmt.Errorf("start %s: %w", e.image, err)
	}

	e.setContainer(id)

	if err = e.waitReady(ctx); err != nil {
		return err
	}

	return e.bootstrap(ctx)
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

	spec := container.Spec{
		Name:   "gopherstack-gcpsql-" + sanitizeName(e.id) + "-" + strconv.Itoa(port),
		Image:  e.image,
		Mounts: []string{e.dir + ":" + dumpMount},
	}

	cport := pgPort

	if e.kind == kindMySQL {
		cport = mysqlPort
		spec.Env = []string{"MYSQL_ROOT_PASSWORD=" + e.pass()}
	} else {
		spec.Env = []string{"POSTGRES_USER=" + pgAdminUser, "POSTGRES_PASSWORD=" + e.pass()}
	}

	spec.Ports = []string{container.PortSpec(container.LoopbackHost, strconv.Itoa(port), cport)}

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

func (e *dockerEngine) addr() string {
	h, p := e.Endpoint()

	return net.JoinHostPort(h, strconv.Itoa(p))
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

func (e *dockerEngine) adminDB() string {
	if e.kind == kindPostgres {
		return pgAdminUser
	}

	return ""
}

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

	e.releasePort()

	_ = os.RemoveAll(e.dir)
}
