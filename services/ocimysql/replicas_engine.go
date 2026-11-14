package ocimysql

import (
	"context"
	"errors"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/mysqlengine"
)

const sourceWait = 5 * time.Minute

var errNoReplication = errors.New("ocimysql: the engine cannot run replication")

// spawnReplica runs work off the lock and then settles the replica: done on success, FAILED otherwise.
func (b *InMemoryBackend) spawnReplica(id, wrID string, work func(ctx context.Context) error, done func(r *Replica)) {
	ctx := b.runCtx

	b.wg.Go(func() {
		err := work(ctx)

		b.mu.Lock("spawnReplica")
		defer b.mu.Unlock()

		r, ok := b.replicas[id]
		if !ok || ctx.Err() != nil {
			return
		}

		r.TimeUpdated, r.Etag, r.TransitionAt, r.After = b.nowUTC(), newEtag(), nil, ""

		if err != nil {
			r.State, r.Details = stateFailed, "Operation failed: "+err.Error()
			b.finishWR(wrID, r.Details)

			return
		}

		r.Details = ""
		done(r)
		b.finishWR(wrID, "")
	})
}

func (b *InMemoryBackend) waitSource(ctx context.Context, sysID string) (Instance, error) {
	ctx, cancel := context.WithTimeout(ctx, sourceWait)
	defer cancel()

	tick := time.NewTicker(sourceTick)
	defer tick.Stop()

	for {
		if inst := b.instance(sysID); inst != nil {
			return inst, nil
		}

		select {
		case <-ctx.Done():
			return nil, errNoServer
		case <-tick.C:
		}
	}
}

const sourceTick = 100 * time.Millisecond

type replicaPlan struct {
	vars  map[string]any
	sysID string
	admin string
	pass  string
	cfgID string
	spec  LaunchSpec
}

func (b *InMemoryBackend) planReplica(id string, fresh bool) (replicaPlan, bool) {
	b.mu.RLock("replicaWork")
	defer b.mu.RUnlock()

	r, ok := b.replicas[id]
	if !ok {
		return replicaPlan{}, false
	}

	s := b.systems[r.DBSystemID]
	cfg := r.config(s)

	return replicaPlan{
		spec: LaunchSpec{
			ID: id, Image: b.eng.imageFor(r.version(s)), RootSecret: r.RootSecret, Fresh: fresh, ServerID: r.ServerID,
		},
		sysID: s.ID, admin: s.AdminUser, pass: s.AdminPass, cfgID: cfg, vars: b.configOverrides(cfg),
	}, true
}

// replicaWork launches the server of a read replica on the source's replication network and attaches it. A fresh
// replica is seeded with the source's data; a relaunched one resumes from its own volume.
func (b *InMemoryBackend) replicaWork(id string, fresh bool) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		p, ok := b.planReplica(id, fresh)
		if !ok {
			return nil
		}

		srcInst, err := b.waitSource(ctx, p.sysID)
		if err != nil {
			return err
		}

		src, ok := srcInst.(Replicable)
		if !ok {
			return errNoReplication
		}

		link, err := src.ReplicationSource(ctx)
		if err != nil {
			return err //nolint:wrapcheck // engine errors carry the cause
		}

		var seed []byte

		if fresh {
			if seed, err = replicaSeed(ctx, src); err != nil {
				return err
			}
		}

		p.spec.Network, p.spec.NetworkID = link.Network, link.NetworkID

		inst, err := b.eng.launcher.Launch(ctx, p.spec)
		if err != nil {
			return err //nolint:wrapcheck // launcher errors carry the cause
		}

		if err = configureAndAttach(ctx, inst, p, link, seed); err != nil {
			inst.Close(ctx, fresh)

			return err
		}

		return b.adoptReplica(ctx, id, inst, fresh, p.cfgID)
	}
}

func configureAndAttach(
	ctx context.Context,
	inst Instance,
	p replicaPlan,
	link mysqlengine.ReplSource,
	seed []byte,
) error {
	if len(p.vars) > 0 {
		if err := configureInstance(ctx, inst, p.vars, false); err != nil {
			return err
		}
	}

	return attachReplica(ctx, inst, p.admin, p.pass, link, seed)
}

func replicaSeed(ctx context.Context, src Replicable) ([]byte, error) {
	dbs, err := src.UserDatabases(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // engine errors carry the cause
	}

	return src.ReplicaSeed(ctx, dbs) //nolint:wrapcheck // engine errors carry the cause
}

func attachReplica(
	ctx context.Context,
	inst Instance,
	user, pass string,
	link mysqlengine.ReplSource,
	seed []byte,
) error {
	rep, ok := inst.(Replicable)
	if !ok {
		return errNoReplication
	}

	if err := inst.EnsureAdmin(ctx, user, pass); err != nil {
		return err //nolint:wrapcheck // engine errors carry the cause
	}

	return rep.AttachReplica(ctx, link, seed) //nolint:wrapcheck // engine errors carry the cause
}

func (b *InMemoryBackend) adoptReplica(ctx context.Context, id string, inst Instance, purge bool, cfgID string) error {
	host, port := inst.Endpoint()

	b.mu.Lock("adoptReplica")
	defer b.mu.Unlock()

	cur, found := b.replicas[id]
	if !found || ctx.Err() != nil {
		go inst.Close(context.WithoutCancel(ctx), purge)

		return nil
	}

	cur.EngineHost, cur.EnginePort, cur.AppliedConfig = host, port, cfgID
	b.eng.live[id] = inst

	return nil
}

// replicaRelaunch restarts a replica's server on its existing volume, for example on another image.
func (b *InMemoryBackend) replicaRelaunch(id, prev string) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		b.mu.Lock("replicaRelaunch")
		old := b.eng.live[id]
		delete(b.eng.live, id)
		b.mu.Unlock()

		if old != nil {
			old.Close(ctx, false)
		}

		if err := b.replicaWork(id, false)(ctx); err != nil {
			return err
		}

		if prev == stateInactive {
			if inst := b.instance(id); inst != nil {
				return inst.Stop(ctx) //nolint:wrapcheck // engine errors carry the cause
			}
		}

		return nil
	}
}

func (b *InMemoryBackend) closeReplica(ctx context.Context, id string, inst Instance) {
	if inst == nil {
		b.eng.launcher.RemoveVolume(ctx, id)

		return
	}

	inst.Close(ctx, true)
}

func (b *InMemoryBackend) stopReplicas(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if inst := b.instance(id); inst != nil {
			if err := inst.Stop(ctx); err != nil {
				return err //nolint:wrapcheck // engine errors carry the cause
			}
		}
	}

	return nil
}

func (b *InMemoryBackend) startReplicas(ctx context.Context, ids []string) error {
	for _, id := range ids {
		if inst := b.instance(id); inst != nil {
			if err := inst.Start(ctx); err != nil {
				return err //nolint:wrapcheck // engine errors carry the cause
			}

			if err := b.syncConfig(ctx, id); err != nil {
				return err
			}

			continue
		}

		if err := b.replicaWork(id, false)(ctx); err != nil {
			return err
		}
	}

	return nil
}

// setReplicaStates moves the replicas listed in ids to state; the caller holds the lock.
func (b *InMemoryBackend) setReplicaStates(ids []string, state string) {
	for _, id := range ids {
		if r, ok := b.replicas[id]; ok && r.State != stateDeleted && r.State != stateDeleting {
			r.State, r.After, r.TransitionAt, r.TimeUpdated, r.Etag = state, "", nil, b.nowUTC(), newEtag()
		}
	}
}

// replicaGuard marks the replicas FAILED when work fails.
func (b *InMemoryBackend) replicaGuard(
	ids []string,
	work func(ctx context.Context) error,
) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		err := work(ctx)
		if err != nil && len(ids) > 0 && ctx.Err() == nil {
			b.mu.Lock("replicaGuard")
			b.setReplicaStates(ids, stateFailed)
			b.mu.Unlock()
		}

		return err
	}
}
