package azuresql

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	tdsPort       = "1433"
	readyInterval = 2 * time.Second
	execTimeout   = 2 * time.Minute
	teardownLimit = 30 * time.Second
	maxErrOutput  = 512
	maxNameLen    = 40
	memoryLimitMB = "2048"
	// sqlcmdScript reads the SQL text from $1 and logs in as sa with the container's own env password, so the
	// sa password never appears on an exec command line. -x stops sqlcmd expanding $(...) in the text.
	sqlcmdScript = `exec /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" ` +
		`-C -b -x -h -1 -W -Q "$1"`
)

type containerRuntime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

type dockerLauncher struct {
	lr      *gcpserve.LazyRuntime
	pool    *portalloc.Allocator
	image   string
	startup time.Duration
}

func newDockerLauncher(
	log *slog.Logger,
	pool *portalloc.Allocator,
	image string,
	startup time.Duration,
) *dockerLauncher {
	lr := gcpserve.NewLazyRuntime(log, "azuresql.runtime")

	return &dockerLauncher{lr: lr, pool: pool, image: image, startup: startup}
}

// Close releases the runtime client.
func (l *dockerLauncher) Close() { _ = l.lr.Close() }

// Launch starts a SQL Server container and waits until it accepts the sa login.
func (l *dockerLauncher) Launch(ctx context.Context, spec LaunchSpec) (Engine, error) {
	r, err := l.lr.Get()
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrRuntimeUnavailable, err)
	}

	rt, ok := r.(containerRuntime)
	if !ok {
		return nil, fmt.Errorf("%w: runtime cannot exec in containers", ErrRuntimeUnavailable)
	}

	e := &dockerEngine{
		rt: rt, pool: l.pool, image: l.image, id: spec.ID, password: spec.Password,
		mu: lockmetrics.New("azuresql.engine"),
	}

	lctx, cancel := context.WithTimeout(ctx, l.startup)
	defer cancel()

	if err = e.boot(lctx, spec); err != nil {
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
	ctrID    string
	port     int
}

func (e *dockerEngine) boot(ctx context.Context, spec LaunchSpec) error {
	id, err := container.CreateAndStartRetry(ctx, e.rt, e.prepare, e.releasePort)
	if err != nil {
		return fmt.Errorf("start %s: %w", e.image, err)
	}

	e.mu.Lock("boot")
	e.ctrID = id
	e.mu.Unlock()

	if err = e.waitReady(ctx); err != nil {
		return err
	}

	if spec.Login == "" {
		return nil
	}

	return e.SetLogin(ctx, spec.Login, spec.Password)
}

func (e *dockerEngine) prepare() (container.Spec, error) {
	port, err := portalloc.AcquireFrom(e.pool, "azuresql")
	if err != nil {
		return container.Spec{}, fmt.Errorf("host port: %w", err)
	}

	e.mu.Lock("prepare")
	e.port = port
	e.mu.Unlock()

	return container.Spec{
		Name:  "gopherstack-azuresql-" + sanitizeName(e.id) + "-" + strconv.Itoa(port),
		Image: e.image,
		Env: []string{
			"ACCEPT_EULA=Y", "MSSQL_SA_PASSWORD=" + e.password, "MSSQL_PID=Developer",
			"MSSQL_MEMORY_LIMIT_MB=" + memoryLimitMB,
		},
		Ports: []string{container.PortSpec(container.LoopbackHost, strconv.Itoa(port), tdsPort)},
	}, nil
}

func (e *dockerEngine) releasePort() {
	e.mu.Lock("releasePort")
	port := e.port
	e.port = 0
	e.mu.Unlock()

	portalloc.ReleaseTo(e.pool, port)
}

func (e *dockerEngine) container() string {
	e.mu.RLock("container")
	defer e.mu.RUnlock()

	return e.ctrID
}

func sanitizeName(s string) string {
	b := []byte(s)
	for i, c := range b {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			b[i] = '-'
		}
	}

	if len(b) > maxNameLen {
		b = b[len(b)-maxNameLen:]
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
		if _, err := e.run(ctx, "SELECT 1"); err == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("sql server did not accept connections: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// run executes one T-SQL batch with sqlcmd inside the container and returns its output. The batch is never
// included in an error because it can carry a password.
func (e *dockerEngine) run(ctx context.Context, text string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, execTimeout)
	defer cancel()

	res, err := e.rt.Exec(ctx, e.container(), []string{"sh", "-c", sqlcmdScript, "sh", text})
	if err != nil {
		return "", fmt.Errorf("sqlcmd exec: %w", err)
	}

	if res.ExitCode != 0 {
		return "", fmt.Errorf("%w: sqlcmd exit %d: %s", ErrInvalid, res.ExitCode, trimOutput(res.Stdout+res.Stderr))
	}

	return res.Stdout, nil
}

func trimOutput(s string) string {
	s = strings.TrimSpace(s)
	if len(s) > maxErrOutput {
		return s[:maxErrOutput]
	}

	return s
}

// SetLogin implements Engine.
func (e *dockerEngine) SetLogin(ctx context.Context, login, password string) error {
	if !validIdent(login) || !validIdent(password) {
		return fmt.Errorf("%w: login or password", ErrInvalid)
	}

	_, err := e.run(ctx, loginSQL(login, password))

	return err
}

// CreateDatabase implements Engine.
func (e *dockerEngine) CreateDatabase(ctx context.Context, name, collation string) error {
	text, err := createDatabaseSQL(name, collation)
	if err != nil {
		return err
	}

	_, err = e.run(ctx, text)

	return err
}

// DropDatabase implements Engine.
func (e *dockerEngine) DropDatabase(ctx context.Context, name string) error {
	text, err := dropDatabaseSQL(name)
	if err != nil {
		return err
	}

	_, err = e.run(ctx, text)

	return err
}

// ListDatabases implements Engine.
func (e *dockerEngine) ListDatabases(ctx context.Context) ([]string, error) {
	out, err := e.run(ctx, listDatabasesSQL)
	if err != nil {
		return nil, err
	}

	var names []string

	for line := range strings.SplitSeq(out, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			names = append(names, line)
		}
	}

	return names, nil
}

// Close implements Engine; it removes the container and releases its port.
func (e *dockerEngine) Close(ctx context.Context) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownLimit)
	defer cancel()

	if id := e.container(); id != "" {
		if err := e.rt.StopAndRemove(ctx, id); err != nil {
			logger.Load(ctx).WarnContext(ctx, "azuresql: container removal failed", "container", id, "error", err)
		}
	}

	e.releasePort()
}
