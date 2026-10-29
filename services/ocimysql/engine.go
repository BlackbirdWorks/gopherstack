package ocimysql

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const enginePrefix = "gopherstack-ocimysql"

var errNoServer = errors.New("ocimysql: the DB system has no running server")

// Instance is one running MySQL server of a DB system.
type Instance interface {
	Endpoint() (string, int)
	EnsureAdmin(ctx context.Context, user, password string) error
	DropUser(ctx context.Context, user string) error
	Stop(ctx context.Context) error
	Start(ctx context.Context) error
	Dump(ctx context.Context) ([]byte, error)
	Restore(ctx context.Context, data []byte) error
	// Close removes the server; purge also deletes its data volume.
	Close(ctx context.Context, purge bool)
}

// LaunchSpec describes the server of one DB system. Fresh discards any earlier data volume.
type LaunchSpec struct {
	ID         string
	Image      string
	RootSecret string
	Fresh      bool
}

// Launcher starts servers.
type Launcher interface {
	Launch(ctx context.Context, spec LaunchSpec) (Instance, error)
	RemoveVolume(ctx context.Context, id string)
}

// DockerConfig configures the docker engine: each DB system runs a mysql container with a named data volume.
type DockerConfig struct {
	Runtime      mysqlengine.Runtime
	Volumes      container.VolumeRuntime
	Ports        *portalloc.Allocator
	Logger       *slog.Logger
	Image        string
	StartTimeout time.Duration
}

type dbEngine struct {
	launcher Launcher
	live     map[string]Instance
	image    string
}

type dockerLauncher struct{ e *mysqlengine.Engine }

func (l dockerLauncher) Launch(ctx context.Context, spec LaunchSpec) (Instance, error) {
	return l.e.Launch(ctx, mysqlengine.Spec{
		ID: spec.ID, Image: spec.Image, RootPassword: spec.RootSecret, Persist: true, Fresh: spec.Fresh,
	})
}

func (l dockerLauncher) RemoveVolume(ctx context.Context, id string) { l.e.RemoveVolume(ctx, id) }

// EnableDockerEngine switches the backend to docker mode. Call it before serving.
func (b *InMemoryBackend) EnableDockerEngine(cfg DockerConfig) error {
	e, err := mysqlengine.New(mysqlengine.Config{
		Runtime: cfg.Runtime, Volumes: cfg.Volumes, Ports: cfg.Ports, Logger: cfg.Logger, Prefix: enginePrefix,
		StartTimeout: cfg.StartTimeout,
	})
	if err != nil {
		return err //nolint:wrapcheck // sentinel from mysqlengine
	}

	b.SetLauncher(dockerLauncher{e: e}, cfg.Image)

	return nil
}

// SetLauncher switches the backend to engine mode with a custom Launcher; image overrides the per-version image.
func (b *InMemoryBackend) SetLauncher(l Launcher, image string) {
	b.mu.Lock("SetLauncher")
	b.eng = &dbEngine{launcher: l, live: map[string]Instance{}, image: image}
	b.mu.Unlock()
}

func (e *dbEngine) imageFor(version string) string {
	if e.image != "" {
		return e.image
	}

	f, _ := familyOf(version)

	return f.image
}

// detachAllLocked removes every live instance from the map and returns them with their ids.
func (b *InMemoryBackend) detachAllLocked() map[string]Instance {
	if b.eng == nil {
		return nil
	}

	out := b.eng.live
	b.eng.live = map[string]Instance{}

	return out
}

func (e *dbEngine) closeAll(lives map[string]Instance, purge bool) {
	if e == nil {
		return
	}

	for _, inst := range lives {
		inst.Close(context.Background(), purge)
	}
}

func (b *InMemoryBackend) instance(id string) Instance {
	b.mu.RLock("instance")
	defer b.mu.RUnlock()

	if b.eng == nil {
		return nil
	}

	return b.eng.live[id]
}

type launchIn struct {
	admin, password string
	dump            []byte
	fresh           bool
}

// launchWork starts (or restarts, on an existing volume) the server of a DB system and records its endpoint.
func (b *InMemoryBackend) launchWork(id string, in launchIn) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		b.mu.RLock("launchWork")
		s, ok := b.systems[id]

		if !ok {
			b.mu.RUnlock()

			return nil
		}

		spec := LaunchSpec{ID: id, Image: b.eng.imageFor(s.Version), RootSecret: s.RootSecret, Fresh: in.fresh}
		b.mu.RUnlock()

		inst, err := b.eng.launcher.Launch(ctx, spec)
		if err != nil {
			return err //nolint:wrapcheck // launcher errors carry the cause
		}

		if err = prepareInstance(ctx, inst, in); err != nil {
			inst.Close(ctx, in.fresh)

			return err
		}

		host, port := inst.Endpoint()

		b.mu.Lock("launchWork")
		defer b.mu.Unlock()

		cur, found := b.systems[id]
		if !found || ctx.Err() != nil {
			go inst.Close(context.WithoutCancel(ctx), in.fresh)

			return nil
		}

		cur.EngineHost, cur.EnginePort = host, port
		b.eng.live[id] = inst

		return nil
	}
}

func prepareInstance(ctx context.Context, inst Instance, in launchIn) error {
	if len(in.dump) > 0 {
		if err := inst.Restore(ctx, in.dump); err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}
	}

	if in.admin == "" {
		return nil
	}

	return inst.EnsureAdmin(ctx, in.admin, in.password) //nolint:wrapcheck // engine errors carry the cause
}

// spawn runs work off the lock and then settles the DB system: done on success, FAILED otherwise.
func (b *InMemoryBackend) spawn(id, wrID string, work func(ctx context.Context) error, done func(s *DBSystem)) {
	ctx := b.runCtx

	b.wg.Go(func() {
		err := work(ctx)

		b.mu.Lock("spawn")
		defer b.mu.Unlock()

		s, ok := b.systems[id]
		if !ok || ctx.Err() != nil {
			return
		}

		s.TimeUpdated, s.Etag, s.TransitionAt = b.nowUTC(), newEtag(), nil

		if err != nil {
			s.State, s.Details, s.After = stateFailed, "Operation failed: "+err.Error(), ""
			b.finishWR(wrID, s.Details)

			return
		}

		s.Details = ""
		done(s)
		b.finishWR(wrID, "")
	})
}

// relaunchLocked restarts the server of every restored DB system that was running.
func (b *InMemoryBackend) relaunchLocked() {
	if b.eng == nil {
		return
	}

	for _, bk := range b.backups {
		if bk.State == stateCreating && bk.TransitionAt == nil {
			bk.State, bk.Details = stateFailed, "Backup interrupted by an emulator restart"
		}
	}

	for _, s := range b.systems {
		if s.State != stateActive && s.State != stateUpdating && s.State != stateCreating {
			continue
		}

		s.State, s.TransitionAt = stateUpdating, nil
		b.spawn(s.ID, "", b.launchWork(s.ID, launchIn{admin: s.AdminUser, password: s.AdminPass}),
			func(s *DBSystem) { s.State = stateActive })
	}
}
