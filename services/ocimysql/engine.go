package ocimysql

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"log/slog"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

const enginePrefix = "gopherstack-ocimysql"

const (
	serverIDMin  = 2
	serverIDSpan = 1<<31 - serverIDMin
)

func randomServerID() uint32 {
	var buf [4]byte

	_, _ = rand.Read(buf[:])

	return binary.BigEndian.Uint32(buf[:])%serverIDSpan + serverIDMin
}

var errNoPITR = errors.New("ocimysql: the engine cannot do point-in-time recovery")

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
	Network    string
	NetworkID  string
	ServerID   uint32
	Fresh      bool
	PublishX   bool
}

// Recoverer is implemented by servers that keep a binary log base for point-in-time recovery.
type Recoverer interface {
	UserDatabases(ctx context.Context) ([]string, error)
	EnablePITR(ctx context.Context, dbs []string) (time.Time, error)
	DisablePITR(ctx context.Context) error
	PrepareRecovery(ctx context.Context, at time.Time) (mysqlengine.Recovery, error)
	ApplyRecovery(ctx context.Context, rec mysqlengine.Recovery) error
}

// XEndpointer is implemented by servers that publish the X protocol port.
type XEndpointer interface {
	XEndpoint() (string, int)
}

// Querier is implemented by servers that answer SQL queries; the first row holds the column names.
type Querier interface {
	Query(ctx context.Context, db, query string) ([][]string, error)
}

// Replicable is implemented by servers that can feed or follow a read replica.
type Replicable interface {
	UserDatabases(ctx context.Context) ([]string, error)
	ReplicationSource(ctx context.Context) (mysqlengine.ReplSource, error)
	ReplicaSeed(ctx context.Context, dbs []string) ([]byte, error)
	AttachReplica(ctx context.Context, src mysqlengine.ReplSource, seed []byte) error
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
		ServerID: spec.ServerID, Network: spec.Network, NetworkID: spec.NetworkID, PublishX: spec.PublishX,
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

	if strings.HasPrefix(version, "9.7.") {
		return "mysql:9.7"
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
	rec             *mysqlengine.Recovery
	admin, password string
	dump            []byte
	fresh           bool
	pitr            bool
}

// launchWork starts (or restarts, on an existing volume) the server of a DB system and records its endpoint.
func (b *InMemoryBackend) launchWork(id string, in launchIn) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		b.mu.Lock("launchWork")
		s, ok := b.systems[id]

		if !ok {
			b.mu.Unlock()

			return nil
		}

		if s.ServerID == 0 {
			s.ServerID = randomServerID()
		}

		spec := LaunchSpec{
			ID: id, Image: b.eng.imageFor(s.Version), RootSecret: s.RootSecret, Fresh: in.fresh, ServerID: s.ServerID,
			PublishX: true,
		}
		cfgID, vars, set := s.ConfigID, b.configOverrides(s.ConfigID), settingsOf(s)
		b.mu.Unlock()

		if in.rec != nil && in.rec.Source != nil {
			spec.Network, spec.NetworkID = in.rec.Source.Network, in.rec.Source.NetworkID
		}

		inst, err := b.eng.launcher.Launch(ctx, spec)
		if err != nil {
			return err //nolint:wrapcheck // launcher errors carry the cause
		}

		base, err := setupInstance(ctx, inst, in, vars, set)
		if err != nil {
			inst.Close(ctx, in.fresh)

			return err
		}

		b.adoptInstance(ctx, id, inst, in.fresh, cfgID, base)

		return nil
	}
}

// setupInstance loads a new server's data and administrator, then applies its configuration and server settings.
func setupInstance(
	ctx context.Context, inst Instance, in launchIn, vars map[string]any, set serverSettings,
) (*time.Time, error) {
	base, err := prepareInstance(ctx, inst, in)
	if err != nil {
		return nil, err
	}

	if len(vars) > 0 {
		if err = configureInstance(ctx, inst, vars, false); err != nil {
			return nil, err
		}
	}

	if !set.isDefault() {
		if err = applyServerSettings(ctx, inst, set); err != nil {
			return nil, err
		}
	}

	return base, nil
}

// adoptInstance records a launched server's endpoints on its DB system.
func (b *InMemoryBackend) adoptInstance(
	ctx context.Context, id string, inst Instance, purge bool, cfgID string, base *time.Time,
) {
	host, port := inst.Endpoint()

	xport := 0
	if xe, isX := inst.(XEndpointer); isX {
		_, xport = xe.XEndpoint()
	}

	b.mu.Lock("launchWork")
	defer b.mu.Unlock()

	cur, found := b.systems[id]
	if !found || ctx.Err() != nil {
		go inst.Close(context.WithoutCancel(ctx), purge)

		return
	}

	cur.EngineHost, cur.EnginePort, cur.EngineXPort, cur.AppliedConfig = host, port, xport, cfgID
	logRecordedFeatures(ctx, id, cur.Extra)
	b.eng.live[id] = inst

	if base != nil {
		cur.PitrBase = base
	}
}

// prepareInstance loads the data a new server starts from and its administrator; the returned time is when
// point-in-time recovery got its base, if it was switched on.
func prepareInstance(ctx context.Context, inst Instance, in launchIn) (*time.Time, error) {
	if len(in.dump) > 0 {
		if err := inst.Restore(ctx, in.dump); err != nil {
			return nil, err //nolint:wrapcheck // engine errors carry the cause
		}
	}

	if in.rec != nil {
		rc, ok := inst.(Recoverer)
		if !ok {
			return nil, errNoPITR
		}

		if err := rc.ApplyRecovery(ctx, *in.rec); err != nil {
			return nil, err //nolint:wrapcheck // engine errors carry the cause
		}
	}

	if in.admin != "" {
		if err := inst.EnsureAdmin(ctx, in.admin, in.password); err != nil {
			return nil, err //nolint:wrapcheck // engine errors carry the cause
		}
	}

	if !in.pitr {
		return nil, nil //nolint:nilnil // no recovery base was taken
	}

	return enablePITR(ctx, inst)
}

func enablePITR(ctx context.Context, inst Instance) (*time.Time, error) {
	rc, ok := inst.(Recoverer)
	if !ok {
		return nil, errNoPITR
	}

	dbs, err := rc.UserDatabases(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // engine errors carry the cause
	}

	t, err := rc.EnablePITR(ctx, dbs)
	if err != nil {
		return nil, err //nolint:wrapcheck // engine errors carry the cause
	}

	return &t, nil
}

// togglePITR switches point-in-time recovery on or off on the running server of a DB system.
func (b *InMemoryBackend) togglePITR(ctx context.Context, id string, on bool) error {
	inst := b.instance(id)
	if inst == nil {
		return errNoServer
	}

	if !on {
		rc, ok := inst.(Recoverer)
		if !ok {
			return errNoPITR
		}

		return rc.DisablePITR(ctx) //nolint:wrapcheck // engine errors carry the cause
	}

	base, err := enablePITR(ctx, inst)
	if err != nil {
		return err
	}

	b.mu.Lock("togglePITR")
	defer b.mu.Unlock()

	if s, ok := b.systems[id]; ok {
		s.PitrBase = base
	}

	return nil
}

// recoveryWork starts a DB system from another one's binary logs, replayed up to the recovery point.
func (b *InMemoryBackend) recoveryWork(id string, p *pitrPlan, in launchIn) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		src, err := b.waitSourceOrStart(ctx, p.srcID)
		if err != nil {
			return err
		}

		rc, ok := src.(Recoverer)
		if !ok {
			return errNoPITR
		}

		rec, err := rc.PrepareRecovery(ctx, p.at)
		if err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}

		in.rec = &rec

		return b.launchWork(id, in)(ctx)
	}
}

// waitSourceOrStart returns the server of a DB system, starting it first when it is stopped.
func (b *InMemoryBackend) waitSourceOrStart(ctx context.Context, id string) (Instance, error) {
	inst := b.instance(id)
	if inst == nil {
		return nil, errNoServer
	}

	b.mu.RLock("waitSourceOrStart")
	s, ok := b.systems[id]
	stopped := ok && s.State == stateInactive
	b.mu.RUnlock()

	if stopped {
		if err := inst.Start(ctx); err != nil {
			return nil, err //nolint:wrapcheck // engine errors carry the cause
		}
	}

	return inst, nil
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

	b.interruptChannels()

	for _, s := range b.systems {
		if s.State != stateActive && s.State != stateUpdating && s.State != stateCreating {
			continue
		}

		s.State, s.TransitionAt = stateUpdating, nil
		b.spawn(s.ID, "", b.launchWork(s.ID, launchIn{admin: s.AdminUser, password: s.AdminPass}),
			func(s *DBSystem) { s.State = stateActive })
	}

	for _, r := range b.replicas {
		if r.State != stateActive && r.State != stateUpdating && r.State != stateCreating {
			continue
		}

		fresh := r.State == stateCreating
		r.State, r.TransitionAt = stateUpdating, nil
		b.spawnReplica(r.ID, "", b.replicaWork(r.ID, fresh), func(r *Replica) { r.State = stateActive })
	}
}
