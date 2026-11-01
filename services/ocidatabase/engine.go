package ocidatabase

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"
)

const closeTimeout = 30 * time.Second

var errPasswordUnknown = errors.New(
	"the ADMIN password is not known to the emulator; update the database with an adminPassword first",
)

// Instance is the running Oracle database of one Autonomous Database.
type Instance interface {
	// Endpoint is the host and port of the database listener.
	Endpoint() (string, int)
	Stop(ctx context.Context) error
	// Start restarts a stopped database and returns once it accepts connections.
	Start(ctx context.Context) error
	SetAdminPassword(ctx context.Context, current, next string) error
	Close(ctx context.Context)
}

// LaunchSpec describes the database of one Autonomous Database. Password is the ADMIN password.
type LaunchSpec struct {
	ID       string
	Image    string
	Password string
}

// Launcher starts databases and returns once they accept connections.
type Launcher interface {
	Launch(ctx context.Context, spec LaunchSpec) (Instance, error)
}

type liveDB struct {
	inst Instance
	g    *gate
	mtls *tlsProxy
	tls  *tlsProxy
}

type dbEngine struct {
	launcher  Launcher
	live      map[string]*liveDB
	passwords map[string]string
	image     string
}

// SetLauncher switches the backend to engine mode: databases created from now on run in launcher.
func (b *InMemoryBackend) SetLauncher(l Launcher, image string) {
	b.mu.Lock("SetLauncher")
	b.eng = &dbEngine{launcher: l, image: image, live: map[string]*liveDB{}, passwords: map[string]string{}}
	b.mu.Unlock()
}

// EngineMode reports the active engine: EngineDocker or EngineStub.
func (b *InMemoryBackend) EngineMode() string {
	b.mu.RLock("EngineMode")
	defer b.mu.RUnlock()

	if b.eng != nil {
		return EngineDocker
	}

	return EngineStub
}

func (b *InMemoryBackend) engineOn(db *ADB) bool { return db.Engine && b.eng != nil }

// detachAll removes every live database from the engine. Callers hold the write lock.
func (b *InMemoryBackend) detachAll() map[string]*liveDB {
	if b.eng == nil {
		return nil
	}

	out := b.eng.live
	b.eng.live, b.eng.passwords = map[string]*liveDB{}, map[string]string{}

	return out
}

func (lv *liveDB) close() {
	lv.mtls.Close()
	lv.tls.Close()

	ctx, cancel := context.WithTimeout(context.Background(), closeTimeout)
	defer cancel()

	lv.inst.Close(ctx)
}

func closeAll(live map[string]*liveDB) {
	for _, lv := range live {
		lv.close()
	}
}

// step describes an operation that moves a database through a transient state.
type step struct {
	work   func(ctx context.Context) error
	typ    string
	action string
	state  string
	target string
}

// begin puts db into step.state and records the work request. Callers hold the write lock.
func (b *InMemoryBackend) begin(db *ADB, s step) *WorkRequest {
	db.State, db.Target, db.Details, db.Etag = s.state, s.target, "", newEtag()
	wr := b.newWR(db.CompartmentID, db.ID, "autonomousdatabase", s.typ, s.action)

	if b.engineOn(db) {
		if s.work == nil {
			s.work = func(context.Context) error { return nil }
		}

		db.TransitionAt = nil
		b.spawn(db.ID, wr.ID, s)

		return wr
	}

	at := b.nowUTC().Add(b.transition)
	db.TransitionAt, wr.FinishAt = &at, &at

	return wr
}

// instant records a work request that is already complete.
func (b *InMemoryBackend) instant(db *ADB, typ string) *WorkRequest {
	wr := b.newWR(db.CompartmentID, db.ID, "autonomousdatabase", typ, actionUpdated)
	b.finishNow(wr)

	return wr
}

func (b *InMemoryBackend) spawn(id, wrID string, s step) {
	ctx := b.runCtx

	b.wg.Go(func() {
		err := s.work(ctx)

		b.mu.Lock("spawn")
		defer b.mu.Unlock()

		db, ok := b.dbs[id]
		if !ok || ctx.Err() != nil {
			return
		}

		wr := b.workReqs[wrID]

		if err != nil {
			db.State, db.Target, db.Etag = stateUnavailable, "", newEtag()
			db.Details = "Operation failed: " + err.Error()

			if wr != nil {
				b.failWR(wr, "OperationFailed", err.Error())
			}

			return
		}

		b.settle(db, s.target)

		if wr != nil {
			b.finishNow(wr)
		}
	})
}

// launchWork starts the database of id, fronts it with the TLS listeners and records it as live.
func (b *InMemoryBackend) launchWork(id string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		b.mu.Lock("launchWork")
		db, ok := b.dbs[id]

		if !ok {
			b.mu.Unlock()

			return nil
		}

		sec, err := b.securityLocked(id)
		pw, image, launcher := b.eng.passwords[id], b.eng.image, b.eng.launcher
		acl, mutual := newACL(db.AccessControl, db.Whitelisted), db.Mtls
		b.mu.Unlock()

		if err != nil {
			return err
		}

		if pw == "" {
			return errPasswordUnknown
		}

		inst, err := launcher.Launch(ctx, LaunchSpec{ID: id, Image: image, Password: pw})
		if err != nil {
			return err //nolint:wrapcheck // launcher errors carry the cause
		}

		lv, err := frontInstance(ctx, inst, sec, acl, mutual)
		if err != nil {
			inst.Close(context.WithoutCancel(ctx))

			return err
		}

		if !b.register(ctx, id, lv) {
			lv.close()
		}

		return nil
	}
}

func frontInstance(ctx context.Context, inst Instance, sec *security, acl *aclRules, mutual bool) (*liveDB, error) {
	g := &gate{sec: sec}
	g.acl.Store(acl)
	g.mtls.Store(mutual)
	setTarget(g, inst)

	mtls, err := startProxy(ctx, g, true)
	if err != nil {
		return nil, err
	}

	tlsOnly, err := startProxy(ctx, g, false)
	if err != nil {
		mtls.Close()

		return nil, err
	}

	return &liveDB{inst: inst, g: g, mtls: mtls, tls: tlsOnly}, nil
}

func setTarget(g *gate, inst Instance) {
	host, port := inst.Endpoint()
	t := net.JoinHostPort(host, strconv.Itoa(port))
	g.target.Store(&t)
}

func (b *InMemoryBackend) liveOf(id string) *liveDB {
	b.mu.RLock("liveOf")
	defer b.mu.RUnlock()

	if b.eng == nil {
		return nil
	}

	return b.eng.live[id]
}

func (b *InMemoryBackend) stopWork(id string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		lv := b.liveOf(id)
		if lv == nil {
			return nil
		}

		lv.g.target.Store(nil)

		return lv.inst.Stop(ctx)
	}
}

func (b *InMemoryBackend) startWork(id string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		lv := b.liveOf(id)
		if lv == nil {
			return b.launchWork(id)(ctx)
		}

		if err := lv.inst.Start(ctx); err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}

		setTarget(lv.g, lv.inst)

		return nil
	}
}

func (b *InMemoryBackend) restartWork(id string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		if err := b.stopWork(id)(ctx); err != nil {
			return err
		}

		return b.startWork(id)(ctx)
	}
}

func (b *InMemoryBackend) terminateWork(id string) func(ctx context.Context) error {
	return func(context.Context) error {
		b.mu.Lock("terminateWork")
		var lv *liveDB

		if b.eng != nil {
			lv = b.eng.live[id]
			delete(b.eng.live, id)
			delete(b.eng.passwords, id)
		}
		b.mu.Unlock()

		if lv != nil {
			lv.close()
		}

		return nil
	}
}

// passwordWork changes the ADMIN password inside the running database, then remembers it.
func (b *InMemoryBackend) passwordWork(id, next string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		lv := b.liveOf(id)
		if lv == nil {
			return nil
		}

		b.mu.RLock("passwordWork")
		current := b.eng.passwords[id]
		b.mu.RUnlock()

		if err := lv.inst.SetAdminPassword(ctx, current, next); err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}

		b.mu.Lock("passwordWork")
		b.eng.passwords[id] = next
		b.mu.Unlock()

		return nil
	}
}

// applyAccess pushes the access control list and mTLS setting of db to its live listeners.
func (b *InMemoryBackend) applyAccess(db *ADB) {
	if b.eng == nil {
		return
	}

	if lv := b.eng.live[db.ID]; lv != nil {
		lv.g.acl.Store(newACL(db.AccessControl, db.Whitelisted))
		lv.g.mtls.Store(db.Mtls)
	}
}

// securityLocked returns the TLS material of db, creating it on first use. Callers hold the write lock.
func (b *InMemoryBackend) securityLocked(id string) (*security, error) {
	if s, ok := b.pki[id]; ok {
		return s, nil
	}

	s, err := newSecurity(b.nowUTC)
	if err != nil {
		return nil, err
	}

	b.pki[id] = s

	return s, nil
}

// register records lv as the live database of id; false when the database is gone or the backend is closing.
func (b *InMemoryBackend) register(ctx context.Context, id string, lv *liveDB) bool {
	b.mu.Lock("register")
	defer b.mu.Unlock()

	if _, found := b.dbs[id]; !found || ctx.Err() != nil {
		return false
	}

	b.eng.live[id] = lv

	return true
}
