package rds

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

type unitOp int

const (
	opStop unitOp = iota
	opStart
	opRestart
)

// runUnitOpLocked starts a background container stop/start/restart for lb; the caller sets the resource status.
func (b *InMemoryBackend) runUnitOpLocked(lb *liveDB, op unitOp) {
	lb.ready = false
	id := lb.containerID

	b.engine.wg.Go(func() { b.execUnitOp(lb, id, op) })
}

func (b *InMemoryBackend) execUnitOp(lb *liveDB, id string, op unitOp) {
	ctx, cancel := lb.opContext(0)
	defer cancel()

	rt := b.engine.cfg.Runtime

	if err := b.stopPhase(ctx, rt, id, op); err != nil {
		b.failUnit(ctx, lb)

		return
	}

	if op == opStop {
		b.markUnitStopped(lb)

		return
	}

	sctx, scancel := context.WithTimeout(ctx, engineOpTimeout)
	err := rt.StartContainer(sctx, id)

	scancel()

	if err != nil {
		b.failUnit(ctx, lb)

		return
	}

	b.awaitAndMark(ctx, lb)
}

func (b *InMemoryBackend) stopPhase(ctx context.Context, rt EngineRuntime, id string, op unitOp) error {
	if op == opStart {
		return nil
	}

	sctx, cancel := context.WithTimeout(ctx, engineStopTimeout)
	defer cancel()

	return rt.StopContainer(sctx, id)
}

func (b *InMemoryBackend) markUnitStopped(lb *liveDB) {
	b.mu.Lock("rdsEngineStopped")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return
	}

	b.unitStatusesLocked(lb, func(inst *DBInstance) {
		if inst.DBInstanceStatus == instanceStatusStopping {
			inst.DBInstanceStatus = instanceStatusStopped
		}
	}, func(c *DBCluster) {
		if c.Status == instanceStatusStopping {
			c.Status = instanceStatusStopped
		}
	})
}

// standaloneUnitLocked returns the unit owned by the instance itself (not its cluster's).
func (b *InMemoryBackend) standaloneUnitLocked(inst *DBInstance) *liveDB {
	if b.engine == nil || inst.DBClusterIdentifier != "" {
		return nil
	}

	return b.engine.units[unitKeyForInstance(inst.DBInstanceIdentifier)]
}

// beginInstanceOpLocked drives a standalone instance's container; handled is false for metadata-only instances.
func (b *InMemoryBackend) beginInstanceOpLocked(inst *DBInstance, op unitOp, status string) (bool, error) {
	lb := b.standaloneUnitLocked(inst)
	if lb == nil {
		return false, nil
	}

	if !lb.ready && op != opStart {
		return true, fmt.Errorf("%w: instance %s is not ready", ErrInvalidDBInstanceState, inst.DBInstanceIdentifier)
	}

	inst.DBInstanceStatus = status
	delete(b.instanceReadyAt, inst.DBInstanceIdentifier)
	b.runUnitOpLocked(lb, op)

	return true, nil
}

// beginClusterOpLocked drives a cluster's container; handled is false for metadata-only clusters.
func (b *InMemoryBackend) beginClusterOpLocked(c *DBCluster, op unitOp, status string) bool {
	lb := b.clusterUnitLocked(c.DBClusterIdentifier)
	if lb == nil {
		return false
	}

	if !lb.ready && op != opStart {
		return true
	}

	c.Status = status
	delete(b.clusterReadyAt, c.DBClusterIdentifier)
	b.runUnitOpLocked(lb, op)

	return true
}

// applyMasterPassword changes the master password inside the running engine for unit key; a no-op when no
// engine backs it.
func (b *InMemoryBackend) applyMasterPassword(key, newPassword string, stateErr error) error {
	if newPassword == "" {
		return nil
	}

	b.mu.RLock("rdsEnginePassword")

	var (
		lb    *liveDB
		login EngineLogin
		set   func(context.Context, EngineLogin, string) error
	)

	if b.engine != nil {
		lb = b.engine.units[key]
	}

	if lb != nil {
		login, set = b.engine.login(lb), b.engine.cfg.SetPassword
	}

	ready := lb != nil && lb.ready
	b.mu.RUnlock()

	if lb == nil {
		return nil
	}

	if !ready {
		return stateErr
	}

	ctx, cancel := lb.opContext(engineOpTimeout)
	defer cancel()

	if err := set(ctx, login, newPassword); err != nil {
		logger.Load(ctx).WarnContext(ctx, "rds: applying master password failed", "code", "DB_ENGINE_PASSWORD_FAILED")

		return stateErr
	}

	b.mu.Lock("rdsEnginePasswordSet")
	lb.password = newPassword
	b.mu.Unlock()

	return nil
}

// relaunchUnitsLocked starts an empty container per restored resource; passwords are not persisted, so each
// relaunched engine gets a random one.
func (b *InMemoryBackend) relaunchUnitsLocked() {
	if b.engine == nil {
		return
	}

	for _, c := range b.clusters.All() {
		if engineKind(c.Engine) == "" || c.Status == instanceStatusDeleting {
			continue
		}

		b.provisionClusterLocked(c, "")
		delete(b.clusterReadyAt, c.DBClusterIdentifier)
	}

	for _, inst := range b.instances.All() {
		if engineKind(inst.Engine) == "" || inst.DBInstanceStatus == instanceStatusDeleting {
			continue
		}

		inst.DBInstanceStatus = instanceStatusCreating

		if b.provisionInstanceLocked(inst, "") {
			delete(b.instanceReadyAt, inst.DBInstanceIdentifier)
		} else {
			inst.DBInstanceStatus = instanceStatusAvailable
		}
	}
}

// engineManaged reports whether docker mode backs the given RDS engine with a real container.
func (b *InMemoryBackend) engineManaged(engine string) bool {
	b.mu.RLock("engineManaged")
	defer b.mu.RUnlock()

	return b.engine != nil && engineKind(engine) != ""
}

// guardEngineModify rejects ModifyDBInstance while a real engine is not up and applies a new master password.
func (b *InMemoryBackend) guardEngineModify(id, password string) error {
	b.mu.RLock("guardEngineModify")

	var key string

	if inst, ok := b.instances.Get(normalizeID(id)); ok {
		if lb := b.instanceUnitLocked(inst); lb != nil {
			key = lb.key
			if !lb.ready {
				b.mu.RUnlock()

				return fmt.Errorf("%w: instance %s is not available", ErrInvalidDBInstanceState, id)
			}
		}
	}

	b.mu.RUnlock()

	if key == "" {
		return nil
	}

	return b.applyMasterPassword(key, password,
		fmt.Errorf("%w: the master password could not be applied to instance %s", ErrInvalidDBInstanceState, id))
}

// guardClusterModify applies a new master password to a cluster's running engine.
func (b *InMemoryBackend) guardClusterModify(id, password string) error {
	if password == "" {
		return nil
	}

	return b.applyMasterPassword(unitKeyForCluster(id), password,
		fmt.Errorf("%w: the master password could not be applied to cluster %s", ErrInvalidDBClusterStateFault, id))
}
