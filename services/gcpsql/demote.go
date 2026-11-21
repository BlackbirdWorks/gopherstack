package gcpsql

import (
	"context"
	"errors"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const opDemoteType = "DEMOTE_MASTER"

// demotion records that a primary now follows an external source; the emulator keeps the instance read-only
// and does not pull data from the source.
type demotion struct {
	Source string `json:"source"`
	At     string `json:"at"`
	Skip   bool   `json:"skipReplicationSetup,omitempty"`
}

// DemoteRequest is the body of instances.demote and instances.demoteMaster.
type DemoteRequest struct {
	Source               string
	SkipReplicationSetup bool
}

// DemoteInstance turns a standalone primary into a read-only replica of an external source.
func (b *InMemoryBackend) DemoteInstance(project, name string, req DemoteRequest) (*Operation, error) {
	if !instanceNameRe.MatchString(req.Source) || len(req.Source) > maxNameLen {
		return nil, errInvalid("Invalid source representative instance name: %q.", req.Source)
	}

	b.mu.Lock("DemoteInstance")
	defer b.mu.Unlock()

	rec, err := b.writableRec(project, name)
	if err != nil {
		return nil, err
	}

	if rec.Inst.State != stateRunnable {
		return nil, errNotRunnable()
	}

	if len(rec.Inst.ReplicaNames) > 0 {
		return nil, errInvalid("An instance with read replicas cannot be demoted; delete the replicas first.")
	}

	if req.Source == name {
		return nil, errInvalid("An instance cannot be demoted to a replica of itself.")
	}

	if _, exists := b.instances[instKey(project, req.Source)]; exists {
		return nil, errInvalid("%q is a Cloud SQL instance, not an on-premises representative instance.", req.Source)
	}

	return b.submitLocked(rec, project, opDemoteType, opMeta{}, func(ctx context.Context) error {
		return b.applyDemotion(ctx, rec, req)
	}), nil
}

func (b *InMemoryBackend) applyDemotion(ctx context.Context, rec *instRec, req DemoteRequest) error {
	b.mu.RLock("DemoteInstance.check")
	stale := rec.gone || b.isReplica(rec)
	b.mu.RUnlock()

	if stale {
		return errNotRunnable()
	}

	if eng := b.engineOf(rec); eng != nil {
		du, ok := eng.(DiskUser)
		if !ok {
			return errNoReplication
		}

		if err := du.SetReadOnly(ctx, true); err != nil {
			return err
		}
	}

	b.mu.Lock("DemoteInstance.task")
	defer b.mu.Unlock()

	rec.Demoted = &demotion{Source: req.Source, At: stamp(b.now()), Skip: req.SkipReplicationSetup}
	rec.Inst.InstanceType = instanceTypeReplica
	rec.Inst.MasterInstanceName = rec.Inst.Project + ":" + req.Source
	rec.Inst.ReplicaConfiguration = map[string]any{keyKind: kindReplicaConfig}
	b.touchLocked(rec)

	logger.Load(ctx).InfoContext(ctx, "gcpsql: instance demoted; replication from the external source is not performed",
		"instance", rec.Inst.Name, "source", req.Source, "skipReplicationSetup", req.SkipReplicationSetup)

	return nil
}

// holdDemoted re-applies read-only mode to a relaunched demoted instance.
func (b *InMemoryBackend) holdDemoted(ctx context.Context, rec *instRec, eng Engine) error {
	b.mu.RLock("holdDemoted")
	demoted := rec.Demoted != nil
	b.mu.RUnlock()

	if !demoted {
		return nil
	}

	if du, ok := eng.(DiskUser); ok {
		return du.SetReadOnly(ctx, true)
	}

	return nil
}

var errNotReplicating = errors.New("the instance follows an external source and has no replication stream to control")

// releaseDemoted makes a demoted instance writable again and drops its external source.
func (b *InMemoryBackend) releaseDemoted(ctx context.Context, rec *instRec) error {
	if eng := b.engineOf(rec); eng != nil {
		if du, ok := eng.(DiskUser); ok {
			if err := du.SetReadOnly(ctx, false); err != nil {
				return err
			}
		}
	}

	b.mu.Lock("releaseDemoted")
	rec.Demoted = nil
	b.mu.Unlock()

	return nil
}
