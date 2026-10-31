package azurearm

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
	"github.com/blackbirdworks/gopherstack/pkgs/pgengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const (
	flexSecretBytes   = 18
	flexPGImagePrefix = "postgres:"
	flexMySQLImage57  = "mysql:5.7"
	flexMySQLImage80  = "mysql:8.0"
	flexMySQLImage84  = "mysql:8.4"
)

var errFlexNoServer = errors.New("azurearm: the flexible server has no running engine")

// DockerFlexConfig configures the docker engine: one postgres or mysql container per server.
type DockerFlexConfig struct {
	// Runtime dials the container runtime on first use.
	Runtime      func() (container.Runtime, error)
	Ports        *portalloc.Allocator
	Logger       *slog.Logger
	Image        string
	StartTimeout time.Duration
}

type flexInstance interface {
	Endpoint() (string, int)
	EnsureAdmin(ctx context.Context, user, password string) error
	CreateDatabase(ctx context.Context, name, charset, collation string) error
	DropDatabase(ctx context.Context, name string) error
	Stop(ctx context.Context) error
	Start(ctx context.Context) error
	Close(ctx context.Context, purge bool)
}

type dockerFlexEngine struct {
	rt   container.Runtime
	live map[string]flexInstance
	pg   *pgengine.Engine
	my   *mysqlengine.Engine
	kind string
	cfg  DockerFlexConfig
	mu   sync.Mutex
}

// NewDockerFlexEngine returns a FlexEngine for kind flexKindPostgres or flexKindMySQL.
func NewDockerFlexEngine(kind string, cfg DockerFlexConfig) FlexEngine {
	return &dockerFlexEngine{cfg: cfg, kind: kind, live: map[string]flexInstance{}}
}

func (e *dockerFlexEngine) init() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.rt != nil {
		return nil
	}

	rt, err := e.cfg.Runtime()
	if err != nil {
		return fmt.Errorf("flexible server engine: container runtime: %w", err)
	}

	switch e.kind {
	case flexKindPostgres:
		e.pg, err = pgengine.New(pgengine.Config{
			Runtime: rt, Ports: e.cfg.Ports, Logger: e.cfg.Logger, Prefix: "azflexpg", StartTimeout: e.cfg.StartTimeout,
		})
	default:
		e.my, err = mysqlengine.New(mysqlengine.Config{
			Runtime:      rt,
			Ports:        e.cfg.Ports,
			Logger:       e.cfg.Logger,
			Prefix:       "azflexmysql",
			StartTimeout: e.cfg.StartTimeout,
		})
	}

	if err != nil {
		return fmt.Errorf("flexible server engine: %w", err)
	}

	e.rt = rt

	return nil
}

func (e *dockerFlexEngine) image(version string) string {
	if e.cfg.Image != "" {
		return e.cfg.Image
	}

	if e.kind == flexKindPostgres {
		return flexPGImagePrefix + version
	}

	switch {
	case strings.HasPrefix(version, "5."):
		return flexMySQLImage57
	case strings.HasPrefix(version, "8.4"):
		return flexMySQLImage84
	}

	return flexMySQLImage80
}

func flexSecret() string {
	b := make([]byte, flexSecretBytes)
	_, _ = rand.Read(b)

	return hex.EncodeToString(b)
}

func (e *dockerFlexEngine) get(id string) (flexInstance, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()

	inst, ok := e.live[id]

	return inst, ok
}

// Ensure implements FlexEngine.
func (e *dockerFlexEngine) Ensure(ctx context.Context, id string, spec FlexSpec) (FlexEndpoint, error) {
	if inst, ok := e.get(id); ok {
		return endpointOf(inst), nil
	}

	if err := e.init(); err != nil {
		return FlexEndpoint{}, err
	}

	inst, err := e.launch(ctx, id, spec)
	if err != nil {
		return FlexEndpoint{}, err
	}

	if spec.AdminLogin != "" && spec.AdminPassword != "" {
		if err = inst.EnsureAdmin(ctx, spec.AdminLogin, spec.AdminPassword); err != nil {
			inst.Close(ctx, true)

			return FlexEndpoint{}, err
		}
	}

	e.mu.Lock()
	e.live[id] = inst
	e.mu.Unlock()

	return endpointOf(inst), nil
}

func endpointOf(inst flexInstance) FlexEndpoint {
	h, p := inst.Endpoint()

	return FlexEndpoint{Host: h, Port: p}
}

func (e *dockerFlexEngine) launch(ctx context.Context, id string, spec FlexSpec) (flexInstance, error) {
	secret := flexSecret()

	if e.kind == flexKindPostgres {
		s, err := e.pg.Launch(ctx, pgengine.Spec{ID: id, Image: e.image(spec.Version), Password: secret})
		if err != nil {
			return nil, fmt.Errorf("launch postgres: %w", err)
		}

		return &pgInstance{s: s, admin: spec.AdminLogin}, nil
	}

	s, err := e.my.Launch(ctx, mysqlengine.Spec{ID: id, Image: e.image(spec.Version), RootPassword: secret})
	if err != nil {
		return nil, fmt.Errorf("launch mysql: %w", err)
	}

	return &mysqlInstance{s: s}, nil
}

// Endpoint implements FlexEngine.
func (e *dockerFlexEngine) Endpoint(id string) (FlexEndpoint, bool) {
	inst, ok := e.get(id)
	if !ok {
		return FlexEndpoint{}, false
	}

	return endpointOf(inst), true
}

func (e *dockerFlexEngine) with(id string, f func(flexInstance) error) error {
	inst, ok := e.get(id)
	if !ok {
		return errFlexNoServer
	}

	return f(inst)
}

// SetPassword implements FlexEngine.
func (e *dockerFlexEngine) SetPassword(ctx context.Context, id, login, password string) error {
	return e.with(id, func(i flexInstance) error { return i.EnsureAdmin(ctx, login, password) })
}

// Stop implements FlexEngine.
func (e *dockerFlexEngine) Stop(ctx context.Context, id string) error {
	return e.with(id, func(i flexInstance) error { return i.Stop(ctx) })
}

// Start implements FlexEngine.
func (e *dockerFlexEngine) Start(ctx context.Context, id string) error {
	return e.with(id, func(i flexInstance) error { return i.Start(ctx) })
}

// Restart implements FlexEngine.
func (e *dockerFlexEngine) Restart(ctx context.Context, id string) error {
	return e.with(id, func(i flexInstance) error {
		if err := i.Stop(ctx); err != nil {
			return err
		}

		return i.Start(ctx)
	})
}

// CreateDatabase implements FlexEngine.
func (e *dockerFlexEngine) CreateDatabase(ctx context.Context, id, name, charset, collation string) error {
	return e.with(id, func(i flexInstance) error { return i.CreateDatabase(ctx, name, charset, collation) })
}

// DropDatabase implements FlexEngine.
func (e *dockerFlexEngine) DropDatabase(ctx context.Context, id, name string) error {
	return e.with(id, func(i flexInstance) error { return i.DropDatabase(ctx, name) })
}

// Delete implements FlexEngine.
func (e *dockerFlexEngine) Delete(ctx context.Context, id string) error {
	e.mu.Lock()
	inst, ok := e.live[id]
	delete(e.live, id)
	e.mu.Unlock()

	if ok {
		inst.Close(ctx, true)
	}

	return nil
}

// Close removes every server and closes the runtime.
func (e *dockerFlexEngine) Close() {
	e.mu.Lock()
	live := e.live
	e.live = map[string]flexInstance{}
	rt := e.rt
	e.mu.Unlock()

	for _, inst := range live {
		inst.Close(context.Background(), true)
	}

	if rt != nil {
		_ = rt.Close()
	}
}

type mysqlInstance struct{ s *mysqlengine.Server }

func (m *mysqlInstance) Endpoint() (string, int) { return m.s.Endpoint() }

func (m *mysqlInstance) EnsureAdmin(ctx context.Context, user, password string) error {
	return m.s.EnsureAdmin(ctx, user, password) //nolint:wrapcheck // reduced to a fixed message by the operation
}

func (m *mysqlInstance) CreateDatabase(ctx context.Context, name, charset, collation string) error {
	return m.s.CreateDatabase(ctx, name, charset, collation) //nolint:wrapcheck // as above
}

func (m *mysqlInstance) DropDatabase(ctx context.Context, name string) error {
	return m.s.DropDatabase(ctx, name) //nolint:wrapcheck // as above
}

func (m *mysqlInstance) Stop(ctx context.Context) error  { return m.s.Stop(ctx) }
func (m *mysqlInstance) Start(ctx context.Context) error { return m.s.Start(ctx) }

func (m *mysqlInstance) Close(ctx context.Context, purge bool) { m.s.Close(ctx, purge) }

type pgInstance struct {
	s     *pgengine.Server
	admin string
}

func (p *pgInstance) Endpoint() (string, int) { return p.s.Endpoint() }

func (p *pgInstance) EnsureAdmin(ctx context.Context, user, password string) error {
	p.admin = user

	return p.s.EnsureAdmin(ctx, user, password) //nolint:wrapcheck // reduced to a fixed message by the operation
}

func (p *pgInstance) CreateDatabase(ctx context.Context, name, charset, collation string) error {
	return p.s.CreateDatabase(ctx, name, p.admin, charset, collation) //nolint:wrapcheck // as above
}

func (p *pgInstance) DropDatabase(ctx context.Context, name string) error {
	return p.s.DropDatabase(ctx, name) //nolint:wrapcheck // as above
}

func (p *pgInstance) Stop(ctx context.Context) error  { return p.s.Stop(ctx) }
func (p *pgInstance) Start(ctx context.Context) error { return p.s.Start(ctx) }

func (p *pgInstance) Close(ctx context.Context, purge bool) { p.s.Close(ctx, purge) }
