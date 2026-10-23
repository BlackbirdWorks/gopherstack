package azuresql

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	maxServers            = 32
	maxDatabasesPerServer = 100
	passwordBytes         = 24
	generatedSuffix       = "aA1!"
)

type server struct {
	engine Engine
	dbs    map[string]Database
	spec   Spec
}

// InMemoryBackend runs one SQL Server container per logical server, or tracks the server state-only when no
// container runtime is reachable. mu guards the map; life serialises lifecycle calls so a slow container start
// does not block reads.
type InMemoryBackend struct {
	mu       *lockmetrics.RWMutex
	life     *lockmetrics.RWMutex
	launcher Launcher
	servers  map[string]*server
	startup  time.Duration
}

// NewInMemoryBackend creates an empty backend with no launcher: servers are state-only.
func NewInMemoryBackend() *InMemoryBackend {
	return &InMemoryBackend{
		mu: lockmetrics.New("azuresql"), life: lockmetrics.New("azuresql.life"),
		servers: map[string]*server{}, startup: DefaultStartupTimeout,
	}
}

// SetLauncher makes new servers start engines through l.
func (b *InMemoryBackend) SetLauncher(l Launcher, startup time.Duration) {
	b.mu.Lock("SetLauncher")
	defer b.mu.Unlock()

	b.launcher, b.startup = l, startup
}

func serverKey(id string) string { return strings.ToLower(id) }

func (b *InMemoryBackend) lookup(id string) (*server, bool) {
	b.mu.RLock("lookup")
	defer b.mu.RUnlock()

	s, ok := b.servers[serverKey(id)]

	return s, ok
}

func (b *InMemoryBackend) launch(ctx context.Context, id string, spec Spec) (Engine, error) {
	b.mu.RLock("launch")
	l := b.launcher
	b.mu.RUnlock()

	if l == nil {
		return nil, nil //nolint:nilnil // state-only server
	}

	pw := spec.AdminPassword
	if pw == "" {
		pw = randomPassword()
	}

	eng, err := l.Launch(ctx, LaunchSpec{ID: id, Login: spec.AdminLogin, Password: pw})
	if errors.Is(err, ErrRuntimeUnavailable) {
		logger.Load(ctx).InfoContext(ctx, "azuresql: no container runtime, server is state-only", "server", id)

		return nil, nil //nolint:nilnil // state-only server
	}

	return eng, err
}

func randomPassword() string {
	buf := make([]byte, passwordBytes)
	_, _ = rand.Read(buf)

	return base64.RawURLEncoding.EncodeToString(buf) + generatedSuffix
}

func endpointOf(s *server) Endpoint {
	if s.engine == nil {
		return Endpoint{}
	}

	host, port := s.engine.Endpoint()

	return Endpoint{Host: host, Port: port}
}

// Ensure creates the server's engine, or applies a changed admin login or password to an existing one.
func (b *InMemoryBackend) Ensure(id string, spec Spec) (Endpoint, error) {
	b.life.Lock("Ensure")
	defer b.life.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), b.startupTimeout())
	defer cancel()

	if s, ok := b.lookup(id); ok {
		return b.update(ctx, s, spec)
	}

	b.mu.RLock("Ensure")
	full := len(b.servers) >= maxServers
	b.mu.RUnlock()

	if full {
		return Endpoint{}, fmt.Errorf("%w: at most %d servers", ErrInvalid, maxServers)
	}

	eng, err := b.launch(ctx, id, spec)
	if err != nil {
		return Endpoint{}, err
	}

	s := &server{engine: eng, spec: spec, dbs: map[string]Database{}}

	b.mu.Lock("Ensure")
	b.servers[serverKey(id)] = s
	b.mu.Unlock()

	return endpointOf(s), nil
}

func (b *InMemoryBackend) startupTimeout() time.Duration {
	b.mu.RLock("startupTimeout")
	defer b.mu.RUnlock()

	return b.startup
}

func (b *InMemoryBackend) update(ctx context.Context, s *server, spec Spec) (Endpoint, error) {
	next := s.spec
	if spec.AdminLogin != "" {
		next.AdminLogin = spec.AdminLogin
	}

	if spec.AdminPassword != "" {
		next.AdminPassword = spec.AdminPassword
	}

	if s.engine != nil && next != s.spec && next.AdminLogin != "" && next.AdminPassword != "" {
		if err := s.engine.SetLogin(ctx, next.AdminLogin, next.AdminPassword); err != nil {
			return Endpoint{}, err
		}
	}

	b.mu.Lock("update")
	s.spec = next
	b.mu.Unlock()

	return endpointOf(s), nil
}

// Endpoint reports where the server's TDS port listens; ok is false for an unknown server.
func (b *InMemoryBackend) Endpoint(id string) (Endpoint, bool) {
	s, ok := b.lookup(id)
	if !ok {
		return Endpoint{}, false
	}

	return endpointOf(s), true
}

// Delete stops the server and releases its port; a missing server is ErrNotFound.
func (b *InMemoryBackend) Delete(id string) error {
	b.life.Lock("Delete")
	defer b.life.Unlock()

	b.mu.Lock("Delete")
	s, ok := b.servers[serverKey(id)]
	delete(b.servers, serverKey(id))
	b.mu.Unlock()

	if !ok {
		return ErrNotFound
	}

	if s.engine != nil {
		s.engine.Close(context.Background())
	}

	return nil
}

// EnsureDatabase creates the database in the engine; an existing database is left as it is.
func (b *InMemoryBackend) EnsureDatabase(id, name, collation string) error {
	b.life.Lock("EnsureDatabase")
	defer b.life.Unlock()

	s, ok := b.lookup(id)
	if !ok {
		return ErrNotFound
	}

	if !validIdent(name) {
		return fmt.Errorf("%w: database name", ErrInvalid)
	}

	b.mu.RLock("EnsureDatabase")
	_, exists := s.dbs[strings.ToLower(name)]
	count := len(s.dbs)
	b.mu.RUnlock()

	if !exists && count >= maxDatabasesPerServer {
		return fmt.Errorf("%w: at most %d databases per server", ErrInvalid, maxDatabasesPerServer)
	}

	if s.engine != nil {
		ctx, cancel := context.WithTimeout(context.Background(), execDeadline)
		defer cancel()

		if err := s.engine.CreateDatabase(ctx, name, collation); err != nil {
			return err
		}
	}

	b.mu.Lock("EnsureDatabase")
	s.dbs[strings.ToLower(name)] = Database{Name: name, Collation: collation}
	b.mu.Unlock()

	return nil
}

const execDeadline = 3 * time.Minute

// DropDatabase drops the database from the engine; a missing database is not an error.
func (b *InMemoryBackend) DropDatabase(id, name string) error {
	b.life.Lock("DropDatabase")
	defer b.life.Unlock()

	s, ok := b.lookup(id)
	if !ok {
		return ErrNotFound
	}

	if s.engine != nil {
		ctx, cancel := context.WithTimeout(context.Background(), execDeadline)
		defer cancel()

		if err := s.engine.DropDatabase(ctx, name); err != nil {
			return err
		}
	}

	b.mu.Lock("DropDatabase")
	delete(s.dbs, strings.ToLower(name))
	b.mu.Unlock()

	return nil
}

// Databases lists the user databases the engine holds, or the tracked ones when the server is state-only.
func (b *InMemoryBackend) Databases(id string) ([]string, error) {
	s, ok := b.lookup(id)
	if !ok {
		return nil, ErrNotFound
	}

	if s.engine != nil {
		ctx, cancel := context.WithTimeout(context.Background(), execDeadline)
		defer cancel()

		return s.engine.ListDatabases(ctx)
	}

	b.mu.RLock("Databases")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(s.dbs))
	for _, d := range s.dbs {
		names = append(names, d.Name)
	}

	slices.Sort(names)

	return names, nil
}

// Reset stops every server.
func (b *InMemoryBackend) Reset() {
	b.life.Lock("Reset")
	defer b.life.Unlock()

	b.mu.Lock("Reset")
	old := b.servers
	b.servers = map[string]*server{}
	b.mu.Unlock()

	closeAll(old)
}

// Close stops every server.
func (b *InMemoryBackend) Close() { b.Reset() }

func closeAll(servers map[string]*server) {
	for _, s := range servers {
		if s.engine != nil {
			s.engine.Close(context.Background())
		}
	}
}
