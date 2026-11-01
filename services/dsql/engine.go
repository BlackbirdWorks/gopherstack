package dsql

import (
	"context"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const engineTeardownLimit = 60 * time.Second

// ClusterRef identifies the cluster a data plane is launched for.
type ClusterRef struct {
	ID        string
	Endpoint  string
	Region    string
	AccountID string
	ARN       string
}

// Instance is one running cluster data plane.
type Instance interface {
	// Addr is the host and port clients connect to.
	Addr() (string, int)
	// Close stops the data plane and releases its resources.
	Close(ctx context.Context)
}

// Launcher starts the data plane of a cluster and returns once the database accepts connections.
type Launcher interface {
	Launch(ctx context.Context, ref ClusterRef) (Instance, error)
}

type unitState int

const (
	unitLaunching unitState = iota
	unitReady
	unitFailed
)

type unit struct {
	inst   Instance
	cancel context.CancelFunc
	state  unitState
}

type engineState struct {
	launcher Launcher
	units    map[string]*unit
	wg       sync.WaitGroup
	closed   bool
}

// EnableEngine switches the backend to launcher-backed clusters: a cluster turns ACTIVE only once its data plane is up.
func (b *InMemoryBackend) EnableEngine(l Launcher) {
	b.mu.Lock("EnableEngine")
	defer b.mu.Unlock()

	b.engine = &engineState{launcher: l, units: make(map[string]*unit)}
}

// ConnectAddr returns where clients reach the cluster's database once its data plane is up.
func (b *InMemoryBackend) ConnectAddr(identifier string) (string, int, bool) {
	b.mu.RLock("ConnectAddr")
	defer b.mu.RUnlock()

	if b.engine == nil {
		return "", 0, false
	}

	u, ok := b.engine.units[identifier]
	if !ok || u.state != unitReady {
		return "", 0, false
	}

	host, port := u.inst.Addr()

	return host, port, true
}

// startUnitLocked launches the data plane of c in the background; callers hold b.mu.
func (b *InMemoryBackend) startUnitLocked(c *Cluster) {
	e := b.engine
	if e == nil || e.closed {
		return
	}

	if _, ok := e.units[c.Identifier]; ok {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	u := &unit{cancel: cancel}
	e.units[c.Identifier] = u
	ref := ClusterRef{ID: c.Identifier, Endpoint: c.Endpoint, Region: c.Region, AccountID: c.AccountID, ARN: c.ARN}

	e.wg.Go(func() {
		inst, err := e.launcher.Launch(ctx, ref)

		b.mu.Lock("engineLaunched")
		current := e.units[ref.ID] == u

		if current {
			if err != nil {
				u.state = unitFailed
			} else {
				u.inst, u.state = inst, unitReady
			}
		}
		b.mu.Unlock()

		if err != nil {
			if ctx.Err() == nil {
				logger.Load(ctx).WarnContext(ctx, "dsql: data plane launch failed", "cluster", ref.ID, "error", err)
			}

			return
		}

		if !current {
			teardown(inst)
		}
	})
}

func teardown(inst Instance) {
	ctx, cancel := context.WithTimeout(context.Background(), engineTeardownLimit)
	defer cancel()

	inst.Close(ctx)
}

// reapUnitLocked stops the data plane of a cluster in the background; callers hold b.mu.
func (b *InMemoryBackend) reapUnitLocked(identifier string) {
	e := b.engine
	if e == nil {
		return
	}

	u, ok := e.units[identifier]
	if !ok {
		return
	}

	delete(e.units, identifier)
	u.cancel()

	if u.inst != nil {
		e.wg.Go(func() { teardown(u.inst) })
	}
}

func (b *InMemoryBackend) reapAllUnitsLocked() {
	if b.engine == nil {
		return
	}

	for id := range b.engine.units {
		b.reapUnitLocked(id)
	}
}

// unitStateLocked reports a cluster's data-plane state; clusters without a unit are ready.
func (b *InMemoryBackend) unitStateLocked(identifier string) unitState {
	if b.engine == nil {
		return unitReady
	}

	u, ok := b.engine.units[identifier]
	if !ok {
		return unitReady
	}

	return u.state
}

// relaunchUnitsLocked starts a data plane for every restored cluster that is not being deleted.
// Data planes are ephemeral, so restored clusters go back through CREATING until they are up.
func (b *InMemoryBackend) relaunchUnitsLocked() {
	if b.engine == nil {
		return
	}

	for _, c := range b.clusters.All() {
		if c.Status == statusDeleting {
			continue
		}

		if c.Status == statusActive || c.Status == statusUpdating {
			c.Status, c.PendingUntil = statusCreating, b.now()
		}

		b.startUnitLocked(c)
	}
}

// Close stops every cluster data plane and waits for the containers to be removed.
func (b *InMemoryBackend) Close() {
	b.mu.Lock("Close")

	e := b.engine
	if e != nil {
		e.closed = true
		b.reapAllUnitsLocked()
	}
	b.mu.Unlock()

	if e != nil {
		e.wg.Wait()
	}
}
