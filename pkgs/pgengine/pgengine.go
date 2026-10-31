// Package pgengine runs real PostgreSQL servers in containers for emulators of managed PostgreSQL services: a named
// data volume, a published loopback port, SQL access as an engine-private superuser, and stop/start that keep data.
package pgengine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	// Superuser is the engine-private role; emulators keep its password to themselves.
	Superuser = "gsroot"

	dataDir       = "/var/lib/postgresql/data"
	pgData        = dataDir + "/pgdata"
	servicePort   = "5432"
	readyInterval = 500 * time.Millisecond
	connectLimit  = 5 * time.Second
	teardownLimit = 30 * time.Second
	volumeLabel   = "gopherstack.pgengine"
	defaultStart  = 3 * time.Minute
	defaultPrefix = "pgengine"
)

// ErrRuntimeUnavailable is returned when no container runtime is configured.
var ErrRuntimeUnavailable = errors.New("pgengine: container runtime unavailable")

var errOp = errors.New("pgengine: operation failed")

// opErr names a failed operation without echoing driver text, which may quote credentials.
func opErr(label string) error { return fmt.Errorf("%w: %s", errOp, label) }

// Runtime is the slice of the container runtime the engine uses.
type Runtime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
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

// Engine launches PostgreSQL servers.
type Engine struct{ cfg Config }

// New validates cfg and returns an Engine.
func New(cfg Config) (*Engine, error) {
	if cfg.Runtime == nil {
		return nil, ErrRuntimeUnavailable
	}

	if cfg.Prefix == "" {
		cfg.Prefix = defaultPrefix
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
	// ID names the container and, when Persist is set, the data volume.
	ID string
	// Image is the container image, e.g. postgres:16.
	Image string
	// Password is the superuser password applied on first initialisation; it must match an existing volume.
	Password string
	Persist  bool
	Fresh    bool
}

// VolumeName is the data volume of server id.
func (e *Engine) VolumeName(id string) string { return e.cfg.Prefix + "-data-" + sanitize(id) }

// RemoveVolume deletes the data volume of server id.
func (e *Engine) RemoveVolume(ctx context.Context, id string) {
	if e.cfg.Volumes != nil {
		_ = e.cfg.Volumes.RemoveVolume(ctx, e.VolumeName(id))
	}
}

// Launch starts a server and waits until it accepts superuser connections.
func (e *Engine) Launch(ctx context.Context, spec Spec) (*Server, error) {
	s := &Server{e: e, spec: spec}
	if e.cfg.Volumes != nil && spec.Persist {
		s.volume = e.VolumeName(spec.ID)
	}

	lctx, cancel := context.WithTimeout(ctx, e.cfg.StartTimeout)
	defer cancel()

	if err := s.boot(lctx); err != nil {
		s.Close(context.WithoutCancel(ctx), false)

		return nil, err
	}

	return s, nil
}

// Server is one running PostgreSQL container.
type Server struct {
	e      *Engine
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
		Name:  s.e.cfg.Prefix + "-" + sanitize(s.spec.ID) + "-" + strconv.Itoa(port),
		Image: s.spec.Image,
		Env:   []string{"POSTGRES_USER=" + Superuser, "POSTGRES_PASSWORD=" + s.spec.Password, "PGDATA=" + pgData},
		Ports: []string{container.PortSpec(container.LoopbackHost, strconv.Itoa(port), servicePort)},
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

func (s *Server) connect(ctx context.Context, db string) (*pgx.Conn, error) {
	h, p := s.Endpoint()

	cfg, err := pgx.ParseConfig("postgres://" + Superuser + "@" + net.JoinHostPort(h, strconv.Itoa(p)) + "/postgres")
	if err != nil {
		return nil, opErr("connection config")
	}

	cfg.Password, cfg.ConnectTimeout = s.spec.Password, connectLimit

	if db != "" {
		cfg.Database = db
	}

	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgengine: connect: %w", err)
	}

	return conn, nil
}

func (s *Server) waitReady(ctx context.Context) error {
	tick := time.NewTicker(readyInterval)
	defer tick.Stop()

	for {
		if conn, err := s.connect(ctx, ""); err == nil {
			_ = conn.Close(ctx)

			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("postgres did not accept connections: %w", ctx.Err())
		case <-tick.C:
		}
	}
}

// Exec runs one statement as the superuser in database db ("" for the default). Callers quote identifiers and
// literals with Ident and Literal.
func (s *Server) Exec(ctx context.Context, db, stmt string) error {
	conn, err := s.connect(ctx, db)
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	if _, err = conn.Exec(ctx, stmt); err != nil {
		return opErr("statement failed")
	}

	return nil
}

// Ident quotes a SQL identifier.
func Ident(name string) string { return pgx.Identifier{name}.Sanitize() }

// Literal quotes a SQL string literal (standard_conforming_strings is on in the official images).
func Literal(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

// EnsureAdmin creates or updates a login role that can create databases and roles and owns what it creates.
func (s *Server) EnsureAdmin(ctx context.Context, user, password string) error {
	conn, err := s.connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	var exists bool
	const roleQuery = "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)"
	if err = conn.QueryRow(ctx, roleQuery, user).Scan(&exists); err != nil {
		return opErr("role lookup failed")
	}

	verb := "CREATE"
	if exists {
		verb = "ALTER"
	}

	stmt := verb + " ROLE " + Ident(user) + " WITH LOGIN CREATEDB CREATEROLE PASSWORD " + Literal(password)
	if _, err = conn.Exec(ctx, stmt); err != nil {
		return opErr("admin role update failed")
	}

	return nil
}

// CreateDatabase creates a database owned by owner; an existing one is left alone.
func (s *Server) CreateDatabase(ctx context.Context, name, owner, charset, collation string) error {
	conn, err := s.connect(ctx, "")
	if err != nil {
		return err
	}
	defer conn.Close(ctx)

	var exists bool
	const dbQuery = "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = $1)"
	if err = conn.QueryRow(ctx, dbQuery, name).Scan(&exists); err != nil {
		return opErr("database lookup failed")
	}

	if exists {
		return nil
	}

	stmt := "CREATE DATABASE " + Ident(name) + " TEMPLATE template0"
	if owner != "" {
		stmt += " OWNER " + Ident(owner)
	}

	if charset != "" {
		stmt += " ENCODING " + Literal(charset)
	}

	if collation != "" {
		stmt += " LC_COLLATE " + Literal(collation) + " LC_CTYPE " + Literal(collation)
	}

	if _, err = conn.Exec(ctx, stmt); err != nil {
		return opErr("create database failed")
	}

	return nil
}

// DropDatabase drops a database, disconnecting its sessions; a missing one is not an error.
func (s *Server) DropDatabase(ctx context.Context, name string) error {
	return s.Exec(ctx, "", "DROP DATABASE IF EXISTS "+Ident(name)+" WITH (FORCE)")
}

// Databases lists the non-template databases.
func (s *Server) Databases(ctx context.Context) ([]string, error) {
	conn, err := s.connect(ctx, "")
	if err != nil {
		return nil, err
	}
	defer conn.Close(ctx)

	rows, err := conn.Query(ctx, "SELECT datname FROM pg_database WHERE NOT datistemplate ORDER BY datname")
	if err != nil {
		return nil, opErr("database list failed")
	}
	defer rows.Close()

	var out []string

	for rows.Next() {
		var n string
		if err = rows.Scan(&n); err != nil {
			return nil, opErr("database list failed")
		}

		out = append(out, n)
	}

	return out, nil
}

// DropRole removes a login role; a missing one is not an error.
func (s *Server) DropRole(ctx context.Context, user string) error {
	return s.Exec(ctx, "", "DROP ROLE IF EXISTS "+Ident(user))
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

// Close removes the container and releases its port; purge also deletes the data volume.
func (s *Server) Close(ctx context.Context, purge bool) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), teardownLimit)
	defer cancel()

	if id := s.container(); id != "" {
		if err := s.e.cfg.Runtime.StopAndRemove(ctx, id); err != nil {
			s.e.cfg.Logger.WarnContext(ctx, "pgengine: container removal failed", "container", id, "error", err)
		}
	}

	s.releasePort()

	if purge && s.volume != "" {
		_ = s.e.cfg.Volumes.RemoveVolume(ctx, s.volume)
	}
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
