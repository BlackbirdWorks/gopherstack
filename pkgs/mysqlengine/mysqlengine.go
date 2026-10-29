// Package mysqlengine runs real MySQL servers in containers for emulators of managed MySQL services: a named data
// volume, a published loopback port, SQL access as root, logical dump and restore, and stop/start that keep data.
package mysqlengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-sql-driver/mysql"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	// RootUser is the container's superuser; emulators keep its password to themselves.
	RootUser = "root"

	dataDir        = "/var/lib/mysql"
	dumpMount      = "/gopherstack-dump"
	servicePort    = "3306"
	readyInterval  = 500 * time.Millisecond
	connectTimeout = 5 * time.Second
	teardownLimit  = 30 * time.Second
	stderrLimit    = 512
	maxDumpBytes   = 256 << 20
	fileMode       = 0o600
	volumeLabel    = "gopherstack.mysqlengine"
	mysqlBin       = "mysql"
	defaultStart   = 3 * time.Minute
)

var (
	// ErrRuntimeUnavailable is returned when no container runtime can exec in and stop containers.
	ErrRuntimeUnavailable = errors.New("mysqlengine: container runtime unavailable")
	errExec               = errors.New("mysqlengine: command failed")
	errTooLarge           = errors.New("mysqlengine: dump is too large")
)

// Runtime is the slice of the container runtime the engine uses.
type Runtime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

// Config configures an Engine. Prefix names containers and volumes ("<prefix>-...").
type Config struct {
	Runtime      Runtime
	Volumes      container.VolumeRuntime
	Ports        *portalloc.Allocator
	Logger       *slog.Logger
	Prefix       string
	StartTimeout time.Duration
}

// Engine launches MySQL servers.
type Engine struct{ cfg Config }

// New validates cfg and returns an Engine.
func New(cfg Config) (*Engine, error) {
	if cfg.Runtime == nil {
		return nil, ErrRuntimeUnavailable
	}

	if cfg.Prefix == "" {
		cfg.Prefix = mysqlBin
	}

	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = defaultStart
	}

	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	return &Engine{cfg: cfg}, nil
}

// Spec describes one server.
type Spec struct {
	// ID names the container and, unless Volume is set, the data volume.
	ID string
	// Image is the container image, e.g. mysql:8.4.
	Image string
	// RootPassword is applied on first initialisation and must match an existing volume.
	RootPassword string
	// Persist keeps data in a named volume; Fresh discards an existing one first.
	Persist bool
	Fresh   bool
}

// VolumeName is the data volume of server id.
func (e *Engine) VolumeName(id string) string { return e.cfg.Prefix + "-data-" + sanitize(id) }

// Launch starts a server and waits until it accepts root connections.
func (e *Engine) Launch(ctx context.Context, spec Spec) (*Server, error) {
	dir, err := os.MkdirTemp("", e.cfg.Prefix+"-")
	if err != nil {
		return nil, fmt.Errorf("dump dir: %w", err)
	}

	s := &Server{e: e, spec: spec, dir: dir}

	if vr := e.cfg.Volumes; vr != nil && spec.Persist {
		s.volume = e.VolumeName(spec.ID)
	}

	lctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	if err = s.boot(lctx); err != nil {
		s.Close(context.WithoutCancel(ctx), false)

		return nil, err
	}

	return s, nil
}

// SweepVolumes deletes unused volumes of this engine that keep does not list.
func (e *Engine) SweepVolumes(ctx context.Context, keep map[string]bool, olderThan time.Duration) {
	vr := e.cfg.Volumes
	if vr == nil {
		return
	}

	names, err := vr.ListUnusedVolumes(ctx, volumeLabel+"."+e.cfg.Prefix, olderThan)
	if err != nil {
		return
	}

	for _, n := range names {
		if !keep[n] && strings.HasPrefix(n, e.cfg.Prefix+"-data-") {
			_ = vr.RemoveVolume(ctx, n)
		}
	}
}

// RemoveVolume deletes the data volume of server id.
func (e *Engine) RemoveVolume(ctx context.Context, id string) {
	if e.cfg.Volumes != nil {
		_ = e.cfg.Volumes.RemoveVolume(ctx, e.VolumeName(id))
	}
}

// Server is one running MySQL container.
type Server struct {
	e      *Engine
	dir    string
	volume string
	ctrID  string
	spec   Spec
	port   int
	mu     sync.Mutex
}

func (s *Server) boot(ctx context.Context) error {
	if s.volume != "" {
		vr := s.e.cfg.Volumes

		if s.spec.Fresh {
			if err := vr.RemoveVolume(ctx, s.volume); err != nil {
				return fmt.Errorf("discard stale volume %s: %w", s.volume, err)
			}
		}

		labels := map[string]string{volumeLabel: "true", volumeLabel + "." + s.e.cfg.Prefix: s.spec.ID}
		if err := vr.CreateVolume(ctx, s.volume, labels); err != nil {
			return fmt.Errorf("create volume %s: %w", s.volume, err)
		}
	}

	id, err := container.CreateAndStartRetry(ctx, s.e.cfg.Runtime, s.prepare, s.releasePort)
	if err != nil {
		return fmt.Errorf("start %s: %w", s.spec.Image, err)
	}

	s.mu.Lock()
	s.ctrID = id
	s.mu.Unlock()

	return s.waitReady(ctx)
}

func (s *Server) prepare() (container.Spec, error) {
	port, err := portalloc.AcquireFrom(s.e.cfg.Ports, s.e.cfg.Prefix)
	if err != nil {
		return container.Spec{}, fmt.Errorf("host port: %w", err)
	}

	s.mu.Lock()
	s.port = port
	s.mu.Unlock()

	spec := container.Spec{
		Name:   s.e.cfg.Prefix + "-" + sanitize(s.spec.ID) + "-" + strconv.Itoa(port),
		Image:  s.spec.Image,
		Env:    []string{"MYSQL_ROOT_PASSWORD=" + s.spec.RootPassword},
		Mounts: []string{s.dir + ":" + dumpMount},
		Ports:  []string{container.PortSpec(container.LoopbackHost, strconv.Itoa(port), servicePort)},
	}

	if s.volume != "" {
		spec.Mounts = append(spec.Mounts, s.volume+":"+dataDir)
	}

	return spec, nil
}

func (s *Server) releasePort() {
	s.mu.Lock()
	port := s.port
	s.port = 0
	s.mu.Unlock()

	portalloc.ReleaseTo(s.e.cfg.Ports, port)
}

func (s *Server) container() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.ctrID
}

// Endpoint is the host and published port clients connect to.
func (s *Server) Endpoint() (string, int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return container.LoopbackHost, s.port
}

func (s *Server) open(db string) (*sql.DB, error) {
	h, p := s.Endpoint()

	cfg := mysql.NewConfig()
	cfg.User, cfg.Passwd, cfg.Net = RootUser, s.spec.RootPassword, "tcp"
	cfg.Addr, cfg.DBName = net.JoinHostPort(h, strconv.Itoa(p)), db
	cfg.InterpolateParams = true
	cfg.Timeout = connectTimeout

	conn, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, fmt.Errorf("mysql config: %w", err)
	}

	return sql.OpenDB(conn), nil
}

func (s *Server) waitReady(ctx context.Context) error {
	tick := time.NewTicker(readyInterval)
	defer tick.Stop()

	for {
		if s.ping(ctx) == nil {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("mysql did not accept connections: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

func (s *Server) ping(ctx context.Context) error {
	db, err := s.open("")
	if err != nil {
		return err
	}
	defer db.Close()

	pctx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	return db.PingContext(pctx)
}

// Exec runs statements in order as root. Placeholders are interpolated client-side, so account DDL accepts them.
func (s *Server) Exec(ctx context.Context, stmt string, args ...any) error {
	db, err := s.open("")
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err = db.ExecContext(ctx, stmt, args...); err != nil {
		return fmt.Errorf("sql: %w", err)
	}

	return nil
}

// Query runs a query as root and returns the rows as strings; the first row holds the column names.
func (s *Server) Query(ctx context.Context, db, query string) ([][]string, error) {
	conn, err := s.open(db)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	rows, err := conn.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("sql: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("sql: %w", err)
	}

	out := [][]string{cols}

	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))

		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err = rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("sql: %w", err)
		}

		row := make([]string, len(cols))
		for i, v := range vals {
			row[i] = v.String
		}

		out = append(out, row)
	}

	return out, rows.Err() //nolint:wrapcheck // driver error
}

// EnsureAdmin creates or updates a full-privilege account reachable from any host.
func (s *Server) EnsureAdmin(ctx context.Context, user, password string) error {
	for _, stmt := range []string{
		"CREATE USER IF NOT EXISTS ?@'%' IDENTIFIED BY ?",
		"ALTER USER ?@'%' IDENTIFIED BY ?",
	} {
		if err := s.Exec(ctx, stmt, user, password); err != nil {
			return err
		}
	}

	return s.Exec(ctx, "GRANT ALL PRIVILEGES ON *.* TO ?@'%' WITH GRANT OPTION", user)
}

// DropUser removes an account; a missing one is not an error.
func (s *Server) DropUser(ctx context.Context, user string) error {
	return s.Exec(ctx, "DROP USER IF EXISTS ?@'%'", user)
}

// Stop stops the container, keeping its data and published port.
func (s *Server) Stop(ctx context.Context) error {
	return s.e.cfg.Runtime.StopContainer(ctx, s.container()) //nolint:wrapcheck // runtime errors carry the id
}

// Start starts a stopped server and waits until it accepts connections.
func (s *Server) Start(ctx context.Context) error {
	if err := s.e.cfg.Runtime.StartContainer(ctx, s.container()); err != nil {
		return err //nolint:wrapcheck // runtime errors carry the id
	}

	return s.waitReady(ctx)
}

func (s *Server) pwdEnv() []string { return []string{"MYSQL_PWD=" + s.spec.RootPassword} }

func (s *Server) run(ctx context.Context, op string, cmd, env []string) error {
	res, err := container.ExecWithEnv(ctx, s.e.cfg.Runtime, s.container(), cmd, env)
	if err != nil {
		return fmt.Errorf("exec %s: %w", op, err)
	}

	if res.ExitCode != 0 {
		msg := strings.TrimSpace(res.Stderr + res.Stdout)
		if len(msg) > stderrLimit {
			msg = msg[:stderrLimit]
		}

		return fmt.Errorf("%w: %s exited %d: %s", errExec, op, res.ExitCode, msg)
	}

	return nil
}

// UserDatabases lists the non-system schemas.
func (s *Server) UserDatabases(ctx context.Context) ([]string, error) {
	rows, err := s.Query(ctx, "", "SHOW DATABASES")
	if err != nil {
		return nil, err
	}

	var out []string

	for _, r := range rows[1:] {
		if !isSystem(r[0]) {
			out = append(out, r[0])
		}
	}

	return out, nil
}

func isSystem(db string) bool {
	switch strings.ToLower(db) {
	case mysqlBin, "sys", "information_schema", "performance_schema":
		return true
	}

	return false
}

// Dump returns a logical dump of every user schema; accounts are not included.
func (s *Server) Dump(ctx context.Context) ([]byte, error) {
	dbs, err := s.UserDatabases(ctx)
	if err != nil || len(dbs) == 0 {
		return nil, err
	}

	path := filepath.Join(s.dir, "dump.sql")
	defer os.Remove(path)

	cmd := append([]string{
		"mysqldump", "-u" + RootUser, "--single-transaction", "--routines",
		"--add-drop-database", "--set-gtid-purged=OFF", "-r", dumpMount + "/dump.sql", "--databases",
	}, dbs...)

	if err = s.run(ctx, "mysqldump", cmd, s.pwdEnv()); err != nil {
		return nil, err
	}

	st, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("dump file: %w", err)
	}

	if st.Size() > maxDumpBytes {
		return nil, errTooLarge
	}

	return os.ReadFile(path)
}

// Restore runs a dump produced by Dump.
func (s *Server) Restore(ctx context.Context, data []byte) error {
	path := filepath.Join(s.dir, "in.sql")
	defer os.Remove(path)

	if err := os.WriteFile(path, data, fileMode); err != nil {
		return fmt.Errorf("write dump: %w", err)
	}

	return s.run(ctx, mysqlBin, []string{
		mysqlBin, "-u" + RootUser, "-e", "source " + dumpMount + "/in.sql",
	}, s.pwdEnv())
}

// Close removes the container and releases its port; purge also deletes the data volume.
func (s *Server) Close(ctx context.Context, purge bool) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownLimit)
	defer cancel()

	if id := s.container(); id != "" {
		if err := s.e.cfg.Runtime.StopAndRemove(ctx, id); err != nil {
			s.e.cfg.Logger.WarnContext(ctx, "mysqlengine: container removal failed", "container", id, "error", err)
		}
	}

	s.releasePort()

	if purge && s.volume != "" {
		_ = s.e.cfg.Volumes.RemoveVolume(ctx, s.volume)
	}

	_ = os.RemoveAll(s.dir)
}

func sanitize(v string) string {
	b := []byte(strings.ToLower(v))
	for i, c := range b {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' {
			b[i] = '-'
		}
	}

	return string(b)
}
