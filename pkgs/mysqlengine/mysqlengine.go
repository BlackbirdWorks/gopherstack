// Package mysqlengine runs real MySQL servers in containers for emulators of managed MySQL services: a named data
// volume, a published loopback port, SQL access as root, logical dump and restore, and stop/start that keep data.
package mysqlengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"maps"
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
	mysqlPort      = 3306
	readyInterval  = 500 * time.Millisecond
	connectTimeout = 5 * time.Second
	teardownLimit  = 30 * time.Second
	stderrLimit    = 512
	maxDumpBytes   = 256 << 20
	fileMode       = 0o600
	volumeLabel    = "gopherstack.mysqlengine"
	mysqlBin       = "mysql"
	defaultStart   = 3 * time.Minute

	argRoutines  = "--routines"
	argAddDrop   = "--add-drop-database"
	argDatabases = "--databases"
	argSingleTx  = "--single-transaction"
)

var (
	// ErrRuntimeUnavailable is returned when no container runtime can exec in and stop containers.
	ErrRuntimeUnavailable = errors.New("mysqlengine: container runtime unavailable")
	errExec               = errors.New("mysqlengine: command failed")
	// ErrTooLarge is returned when a dump or query result exceeds its size limit.
	ErrTooLarge = errors.New("mysqlengine: result is too large")
)

// Runtime is the slice of the container runtime the engine uses.
type Runtime interface {
	CreateAndStart(ctx context.Context, spec container.Spec) (string, error)
	StopAndRemove(ctx context.Context, containerID string) error
	StopContainer(ctx context.Context, containerID string) error
	StartContainer(ctx context.Context, containerID string) error
	Exec(ctx context.Context, containerID string, cmd []string) (container.ExecResult, error)
}

// Config configures an Engine. Prefix names containers and volumes ("<prefix>-..."); Service owns ports and
// orphan labels (default Prefix); ReplUser is the replication account created on a source.
type Config struct {
	Runtime      Runtime
	Volumes      container.VolumeRuntime
	Ports        *portalloc.Allocator
	Logger       *slog.Logger
	Prefix       string
	Service      string
	ReplUser     string
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

	if cfg.Service == "" {
		cfg.Service = cfg.Prefix
	}

	if cfg.ReplUser == "" {
		cfg.ReplUser = "replica"
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
	VolumeLabels map[string]string
	// Labels are added to the container. Resource, when set, labels the container as owned by that key so a
	// restarted gopherstack can find it; Adopt then lets Launch take over such an orphan of an earlier run.
	Labels map[string]string
	// ID names the container and, unless Volume is set, the data volume.
	ID string
	// Image is the container image, e.g. mysql:8.4.
	Image string
	// RootPassword is applied on first initialisation and must match an existing volume.
	RootPassword string
	// Volume overrides the data volume name; VolumeLabels are added to the volume's labels.
	Volume   string
	Resource string
	// Network and NetworkID place the container on an existing replication network.
	Network   string
	NetworkID string
	// MemoryBytes and NanoCPUs limit the container.
	MemoryBytes int64
	NanoCPUs    int64
	// ServerID, when non-zero, starts the server replication-ready (GTID mode).
	ServerID uint32
	// Persist keeps data in a named volume; Fresh discards an existing one first.
	Persist bool
	Fresh   bool
	Adopt   bool
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
		if spec.Volume != "" {
			s.volume = spec.Volume
		}
	}

	s.netID = spec.NetworkID

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
	net    *sourceNet
	dir    string
	volume string
	ctrID  string
	ctrNm  string
	netID  string

	replPass string
	spec     Spec
	port     int
	mu       sync.Mutex

	promoted bool
	attached bool
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
		maps.Copy(labels, s.spec.VolumeLabels)

		if err := vr.CreateVolume(ctx, s.volume, labels); err != nil {
			return fmt.Errorf("create volume %s: %w", s.volume, err)
		}
	}

	if !s.adopt(ctx) {
		id, err := container.CreateAndStartRetry(ctx, s.e.cfg.Runtime, s.prepare, s.releasePort)
		if err != nil {
			return fmt.Errorf("start %s: %w", s.spec.Image, err)
		}

		s.mu.Lock()
		s.ctrID = id
		s.mu.Unlock()
	}

	return s.waitReady(ctx)
}

func (s *Server) prepare() (container.Spec, error) {
	port, err := portalloc.AcquireFrom(s.e.cfg.Ports, s.e.cfg.Service)
	if err != nil {
		return container.Spec{}, fmt.Errorf("host port: %w", err)
	}

	name := s.e.cfg.Prefix + "-" + sanitize(s.spec.ID) + "-" + strconv.Itoa(port)

	s.mu.Lock()
	s.port, s.ctrNm = port, name
	s.mu.Unlock()

	spec := container.Spec{
		Name:        name,
		Image:       s.spec.Image,
		Labels:      s.ownerLabels(),
		Env:         []string{"MYSQL_ROOT_PASSWORD=" + s.pass()},
		Mounts:      []string{s.dir + ":" + dumpMount},
		Ports:       []string{container.PortSpec(container.LoopbackHost, strconv.Itoa(port), servicePort)},
		Network:     s.spec.Network,
		MemoryBytes: s.spec.MemoryBytes,
		NanoCPUs:    s.spec.NanoCPUs,
	}

	if s.spec.ServerID > 0 {
		spec.Cmd = replicationFlags(s.spec.ServerID)
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
	cfg.User, cfg.Passwd, cfg.Net = RootUser, s.pass(), "tcp"
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

// exec runs one statement as root. Placeholders are interpolated client-side, so account DDL accepts them.
func (s *Server) exec(ctx context.Context, stmt string, args ...any) error {
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
	return s.QueryMax(ctx, db, query, 0)
}

// QueryMax is Query that fails with ErrTooLarge once the result exceeds maxCells values (zero is unlimited).
func (s *Server) QueryMax(ctx context.Context, db, query string, maxCells int) ([][]string, error) {
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

		if out = append(out, row); maxCells > 0 && len(out)*len(cols) > maxCells {
			return nil, ErrTooLarge
		}
	}

	return out, rows.Err() //nolint:wrapcheck // driver error
}

// EnsureAdmin creates or updates a full-privilege account reachable from any host.
func (s *Server) EnsureAdmin(ctx context.Context, user, password string) error {
	for _, stmt := range []string{
		"CREATE USER IF NOT EXISTS ?@'%' IDENTIFIED BY ?",
		"ALTER USER ?@'%' IDENTIFIED BY ?",
	} {
		if err := s.exec(ctx, stmt, user, password); err != nil {
			return err
		}
	}

	return s.exec(ctx, "GRANT ALL PRIVILEGES ON *.* TO ?@'%' WITH GRANT OPTION", user)
}

const (
	identMaxLen  = 64
	charsetChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_"
	dbNameChars  = charsetChars + "$-"
)

// ErrInvalidIdentifier reports a database name, charset or collation outside the allowed character set.
var ErrInvalidIdentifier = errors.New("mysqlengine: invalid identifier")

// rebuildIdent copies s byte by byte from the constant alphabet, so the result shares no data with the input.
func rebuildIdent(s, alphabet string) (string, error) {
	if s == "" || len(s) > identMaxLen {
		return "", ErrInvalidIdentifier
	}

	var b strings.Builder

	b.Grow(len(s))

	for i := range len(s) {
		idx := strings.IndexByte(alphabet, s[i])
		if idx < 0 {
			return "", ErrInvalidIdentifier
		}

		b.WriteByte(alphabet[idx])
	}

	return b.String(), nil
}

// CreateDatabaseSQL builds CREATE DATABASE from identifiers rebuilt out of a fixed alphabet.
func CreateDatabaseSQL(name, charset, collation string) (string, error) {
	n, err := rebuildIdent(name, dbNameChars)
	if err != nil {
		return "", err
	}

	cs, err := rebuildIdent(charset, charsetChars)
	if err != nil {
		return "", err
	}

	co, err := rebuildIdent(collation, charsetChars)
	if err != nil {
		return "", err
	}

	return "CREATE DATABASE IF NOT EXISTS `" + n + "` CHARACTER SET " + cs + " COLLATE " + co, nil
}

// DropDatabaseSQL builds DROP DATABASE from a name rebuilt out of a fixed alphabet.
func DropDatabaseSQL(name string) (string, error) {
	n, err := rebuildIdent(name, dbNameChars)
	if err != nil {
		return "", err
	}

	return "DROP DATABASE IF EXISTS `" + n + "`", nil
}

// CreateDatabase creates a database with the given charset and collation.
func (s *Server) CreateDatabase(ctx context.Context, name, charset, collation string) error {
	stmt, err := CreateDatabaseSQL(name, charset, collation)
	if err != nil {
		return err
	}

	return s.exec(ctx, stmt)
}

// DropDatabase removes a database; a missing one is not an error.
func (s *Server) DropDatabase(ctx context.Context, name string) error {
	stmt, err := DropDatabaseSQL(name)
	if err != nil {
		return err
	}

	return s.exec(ctx, stmt)
}

// DropUser removes an account; a missing one is not an error.
func (s *Server) DropUser(ctx context.Context, user string) error {
	return s.exec(ctx, "DROP USER IF EXISTS ?@'%'", user)
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

	if err := s.waitReady(ctx); err != nil {
		return err
	}

	s.reguard(ctx)

	return nil
}

func (s *Server) pwdEnv() []string { return []string{"MYSQL_PWD=" + s.pass()} }

func (s *Server) run(ctx context.Context, op string, cmd, env []string) error {
	_, err := s.runOut(ctx, op, cmd, env)

	return err
}

func (s *Server) runOut(ctx context.Context, op string, cmd, env []string) (string, error) {
	res, err := container.ExecWithEnv(ctx, s.e.cfg.Runtime, s.container(), cmd, env)
	if err != nil {
		return "", fmt.Errorf("%w: exec %s: %s", errExec, op, s.scrub(err.Error()))
	}

	if res.ExitCode != 0 {
		msg := s.scrub(strings.TrimSpace(res.Stderr + res.Stdout))
		if len(msg) > stderrLimit {
			msg = msg[:stderrLimit]
		}

		return "", fmt.Errorf("%w: %s exited %d: %s", errExec, op, res.ExitCode, msg)
	}

	return res.Stdout, nil
}

// scrub redacts the server's credentials from command output.
func (s *Server) scrub(msg string) string {
	s.mu.Lock()
	secrets := []string{s.spec.RootPassword, s.replPass}
	s.mu.Unlock()

	for _, sec := range secrets {
		if sec != "" {
			msg = strings.ReplaceAll(msg, sec, "***")
		}
	}

	return msg
}

func (s *Server) pass() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.spec.RootPassword
}

// SetRootPassword records a root password changed through SQL so later connections use it.
func (s *Server) SetRootPassword(pw string) {
	s.mu.Lock()
	s.spec.RootPassword = pw
	s.mu.Unlock()
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
	if err != nil {
		return nil, err
	}

	return s.DumpDatabases(ctx, dbs)
}

// DumpDatabases returns a logical dump of dbs; accounts are not included.
func (s *Server) DumpDatabases(ctx context.Context, dbs []string) ([]byte, error) {
	if len(dbs) == 0 {
		return nil, nil
	}

	path := filepath.Join(s.dir, "dump.sql")
	defer os.Remove(path)

	cmd := append([]string{
		mysqldumpBin, "-u" + RootUser, argSingleTx, argRoutines,
		argAddDrop, "--set-gtid-purged=OFF", "-r", dumpMount + "/dump.sql", argDatabases,
	}, dbs...)

	if err := s.run(ctx, "mysqldump", cmd, s.pwdEnv()); err != nil {
		return nil, err
	}

	st, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("dump file: %w", err)
	}

	if st.Size() > maxDumpBytes {
		return nil, ErrTooLarge
	}

	return os.ReadFile(path)
}

// Restore runs a dump produced by Dump.
func (s *Server) Restore(ctx context.Context, data []byte) error {
	return s.RestoreInto(ctx, "", data)
}

// RestoreInto runs a SQL file with db as the default database when db is not empty.
func (s *Server) RestoreInto(ctx context.Context, db string, data []byte) error {
	path := filepath.Join(s.dir, "in.sql")
	defer os.Remove(path)

	if err := os.WriteFile(path, data, fileMode); err != nil {
		return fmt.Errorf("write dump: %w", err)
	}

	cmd := []string{mysqlBin, "-u" + RootUser}
	if db != "" {
		cmd = append(cmd, "--database="+db)
	}

	return s.run(ctx, mysqlBin, append(cmd, "-e", "source "+dumpMount+"/in.sql"), s.pwdEnv())
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

	s.releaseNetwork(ctx)
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
