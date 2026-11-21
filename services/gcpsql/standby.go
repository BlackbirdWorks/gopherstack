package gcpsql

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	availabilityRegional = "REGIONAL"
	kindReplicaConfig    = "sql#replicaConfiguration"
)

// standbyWantedLocked reports whether rec should run a hot standby beside its primary engine.
func (b *InMemoryBackend) standbyWantedLocked(rec *instRec) bool {
	return rec.eng != nil && !b.isReplica(rec) && rec.Inst.State != stateDeleting &&
		settingString(rec.Inst.Settings, "availabilityType") == availabilityRegional
}

// reconcileStandby starts or removes the standby engine so it matches availabilityType; rebuild forces a fresh one
// after a change the standby cannot follow (flags, tier).
func (b *InMemoryBackend) reconcileStandby(ctx context.Context, rec *instRec, rebuild bool) {
	b.mu.Lock("reconcileStandby")
	want, have := b.standbyWantedLocked(rec), rec.standby

	if have != nil && (!want || rebuild) {
		rec.standby = nil
	} else {
		have = nil
	}
	started := rec.standby != nil
	b.mu.Unlock()

	purgeEngine(ctx, have)

	if want && !started {
		b.launchStandby(ctx, rec)
	}
}

// dropStandby stops and deletes the standby engine of rec.
func (b *InMemoryBackend) dropStandby(ctx context.Context, rec *instRec) {
	b.mu.Lock("dropStandby")
	sb := rec.standby
	rec.standby = nil
	b.mu.Unlock()

	purgeEngine(ctx, sb)
}

// launchStandby seeds a hot standby from the active engine. Without a runtime that can run replication the
// instance stays HA in its records only, as engine-less instances do.
func (b *InMemoryBackend) launchStandby(ctx context.Context, rec *instRec) {
	b.mu.RLock("launchStandby")
	eng, l, slot := rec.eng, b.launcher, rec.Slot
	spec := LaunchSpec{
		ID: rec.Inst.Name, Project: rec.Inst.Project, Version: rec.Inst.DatabaseVersion, Password: rec.RootPassword,
		ServerID: randomServerID(), Persist: true, Fresh: true, Standby: true, Slot: otherSlot(slot),
	}
	spec.NanoCPUs, spec.MemoryBytes = tierLimits(settingString(rec.Inst.Settings, "tier"))
	b.mu.RUnlock()

	rp, ok := eng.(Replicator)
	if !ok || l == nil {
		return
	}

	log := logger.Load(ctx)

	src, err := rp.ReplicationSource(ctx)
	if err != nil {
		log.WarnContext(ctx, "gcpsql: no standby, replication unavailable", "instance", rec.Inst.Name, "error", err)

		return
	}

	spec.Replica = &src

	sb, err := b.launch(ctx, l, spec)
	if err != nil || sb == nil {
		log.WarnContext(ctx, "gcpsql: standby did not start", "instance", rec.Inst.Name, "error", err)

		return
	}

	if err = b.attachReplica(ctx, rec, sb, spec, eng); err != nil {
		log.WarnContext(ctx, "gcpsql: standby could not follow the primary", "instance", rec.Inst.Name, "error", err)
		purgeEngine(ctx, sb)

		return
	}

	b.mu.Lock("launchStandby")
	if rec.gone || rec.standby != nil || rec.eng != eng {
		b.mu.Unlock()
		purgeEngine(ctx, sb)

		return
	}

	rec.standby = sb
	b.mu.Unlock()
}

// handOver moves the writer role from old to next without losing a committed write: old rejects writes, next
// applies everything old committed and is promoted.
func (b *InMemoryBackend) handOver(ctx context.Context, old, next Engine) error {
	fo, ok := old.(Failoverable)
	if !ok {
		return errNoReplication
	}

	nextRepl, ok := next.(Replicator)
	if !ok {
		return errNoReplication
	}

	nextFO, ok := next.(Failoverable)
	if !ok {
		return errNoReplication
	}

	pos, err := fo.Quiesce(ctx)
	if err != nil {
		_ = fo.Unquiesce(ctx)

		return fmt.Errorf("quiesce the primary: %w", err)
	}

	if err = nextFO.WaitCaughtUp(ctx, pos); err != nil {
		_ = fo.Unquiesce(ctx)

		return err
	}

	if err = fo.Seal(ctx); err == nil {
		err = nextFO.Settle(ctx)
	}

	if err != nil {
		_ = fo.Unquiesce(ctx)

		return err
	}

	if err = nextRepl.Promote(ctx); err != nil {
		_ = fo.Unquiesce(ctx)

		return fmt.Errorf("promote: %w", err)
	}

	return nil
}

// repointReplicas aims the read replicas named in names at src's new writer.
func (b *InMemoryBackend) repointReplicas(ctx context.Context, project string, names []string, writer Engine) error {
	rp, ok := writer.(Replicator)
	if !ok {
		return errNoReplication
	}

	var src ReplicaSource

	have := false

	for _, n := range names {
		b.mu.RLock("repointReplicas")
		rrec := b.instances[instKey(project, n)]

		var reng Engine
		if rrec != nil && !rrec.gone {
			reng = rrec.eng
		}
		b.mu.RUnlock()

		rr, isRepl := reng.(Replicator)
		if !isRepl {
			continue
		}

		if !have {
			var err error
			if src, err = rp.ReplicationSource(ctx); err != nil {
				return err
			}

			have = true
		}

		if err := rr.AttachReplica(ctx, src, nil); err != nil {
			return fmt.Errorf("re-point replica %s: %w", n, err)
		}
	}

	return nil
}

// Failover promotes the standby of a REGIONAL instance; data committed before the call is present afterwards.
func (b *InMemoryBackend) Failover(project, instance string, settingsVersion string) (*Operation, error) {
	b.mu.Lock("Failover")
	defer b.mu.Unlock()

	rec, err := b.opRec(project, instance)
	if err != nil {
		return nil, err
	}

	regional := settingString(rec.Inst.Settings, "availabilityType") == availabilityRegional
	if !regional || rec.Inst.MasterInstanceName != "" {
		return nil, errInvalid("Failover requires a high availability (REGIONAL) primary instance.")
	}

	if settingsVersion != "" && settingsVersion != settingString(rec.Inst.Settings, "settingsVersion") {
		return nil, newErr(http.StatusPreconditionFailed, "conditionNotMet", "Precondition check failed.")
	}

	return b.submitLocked(rec, project, "FAILOVER", opMeta{}, func(ctx context.Context) error {
		return b.failoverTask(ctx, rec)
	}), nil
}

func (b *InMemoryBackend) failoverTask(ctx context.Context, rec *instRec) error {
	b.mu.RLock("failoverTask")
	old, next := rec.eng, rec.standby
	names := slices.Clone(rec.Inst.ReplicaNames)
	b.mu.RUnlock()

	if old != nil {
		if next == nil {
			return fmt.Errorf("%w: the standby is not running", errExec)
		}

		if err := b.handOver(ctx, old, next); err != nil {
			return err
		}

		repointErr := b.repointReplicas(ctx, rec.Inst.Project, names, next)

		b.mu.Lock("failoverTask")
		rec.eng, rec.standby, rec.Slot = next, nil, otherSlot(rec.Slot)
		rec.Inst.IPAddresses, rec.Inst.GopherstackEndpoint = addresses(b.clientEndpoint(rec))
		b.mu.Unlock()

		retireWriter(ctx, old)

		b.finishPromotion(ctx, rec, next)

		if repointErr != nil {
			return repointErr
		}
	}

	b.mu.Lock("failoverTask")
	rec.Inst.GceZone, rec.Inst.SecondaryGceZone = rec.Inst.SecondaryGceZone, rec.Inst.GceZone
	b.touchLocked(rec)
	b.mu.Unlock()

	return nil
}

// finishPromotion brings a new writer's log archiving and standby in line with the instance settings.
func (b *InMemoryBackend) finishPromotion(ctx context.Context, rec *instRec, eng Engine) {
	if err := b.syncPITR(ctx, rec, eng); err != nil {
		logger.Load(ctx).WarnContext(ctx, "gcpsql: point-in-time recovery not restored after hand-over",
			"instance", rec.Inst.Name, "error", err)
	}

	b.reconcileStandby(ctx, rec, false)
}

// Switchover promotes the read replica and makes the old primary a replica of it.
func (b *InMemoryBackend) Switchover(project, instance string) (*Operation, error) {
	b.mu.Lock("Switchover")
	defer b.mu.Unlock()

	rep, err := b.replicaOnly(project, instance)
	if err != nil {
		return nil, err
	}

	if _, err = b.switchoverPrimaryLocked(rep); err != nil {
		return nil, err
	}

	return b.submitLocked(rep, project, "SWITCHOVER", opMeta{}, func(ctx context.Context) error {
		return b.switchoverTask(ctx, rep)
	}), nil
}

func (b *InMemoryBackend) switchoverPrimaryLocked(rep *instRec) (*instRec, error) {
	name, err := masterName(rep.Inst.Project, rep.Inst.MasterInstanceName)
	if err != nil {
		return nil, err
	}

	prim, err := b.lookupLocked(rep.Inst.Project, name)
	if err != nil {
		return nil, err
	}

	if b.isReplica(prim) {
		return nil, errInvalid("Switchover needs a replica of a primary instance, not of another replica.")
	}

	if prim.Inst.State != stateRunnable || rep.Inst.State != stateRunnable {
		return nil, errNotRunnable()
	}

	return prim, nil
}

func (b *InMemoryBackend) switchoverTask(ctx context.Context, rep *instRec) error {
	b.mu.Lock("switchoverTask")
	prim, err := b.switchoverPrimaryLocked(rep)
	if err != nil {
		b.mu.Unlock()

		return err
	}

	oldEng, repEng, standby := prim.eng, rep.eng, prim.standby
	siblings := slices.DeleteFunc(
		slices.Clone(prim.Inst.ReplicaNames),
		func(n string) bool { return n == rep.Inst.Name },
	)
	b.mu.Unlock()

	if oldEng != nil && repEng != nil {
		if err = b.handOver(ctx, oldEng, repEng); err != nil {
			return err
		}
	}

	b.mu.Lock("switchoverTask")
	b.swapRolesLocked(prim, rep, siblings)
	prim.eng, prim.standby, prim.Slot, prim.fresh = nil, nil, "", true
	b.mu.Unlock()

	purgeEngine(ctx, standby)
	purgeEngine(ctx, oldEng)

	var repointErr error
	if repEng != nil {
		repointErr = b.repointReplicas(ctx, rep.Inst.Project, siblings, repEng)
	}

	b.finishPromotion(ctx, rep, repEng)

	if err = b.rebuildAsReplica(ctx, prim); err != nil {
		return err
	}

	return repointErr
}

// swapRolesLocked rewrites the records of a primary and its replica after a switchover.
func (b *InMemoryBackend) swapRolesLocked(prim, rep *instRec, siblings []string) {
	project := prim.Inst.Project

	rep.Inst.InstanceType, rep.Inst.MasterInstanceName, rep.Inst.ReplicaConfiguration = instanceTypePrimary, "", nil
	rep.ReplicaPaused = false
	rep.Inst.ReplicaNames = append(slices.Clone(siblings), prim.Inst.Name)
	slices.Sort(rep.Inst.ReplicaNames)

	prim.Inst.InstanceType = instanceTypeReplica
	prim.Inst.MasterInstanceName = project + ":" + rep.Inst.Name
	prim.Inst.ReplicaConfiguration = map[string]any{keyKind: kindReplicaConfig}
	prim.Inst.ReplicaNames = nil

	for _, n := range siblings {
		if s, ok := b.instances[instKey(project, n)]; ok {
			s.Inst.MasterInstanceName = project + ":" + rep.Inst.Name
			b.touchLocked(s)
		}
	}

	swapCluster(prim, rep)
	b.touchLocked(prim)
	b.touchLocked(rep)
}

// swapCluster moves the DR replica designation along with the writer role.
func swapCluster(oldPrim, newPrim *instRec) {
	if oldPrim.Inst.ReplicationCluster == nil ||
		oldPrim.Inst.ReplicationCluster.FailoverDrReplicaName != newPrim.Inst.Name {
		return
	}

	oldPrim.Inst.ReplicationCluster = &ReplicationCluster{DrReplica: true}
	newPrim.Inst.ReplicationCluster = &ReplicationCluster{FailoverDrReplicaName: oldPrim.Inst.Name}
}

// rebuildAsReplica relaunches rec, whose engine was removed, as a replica of its new primary on rec's own queue.
func (b *InMemoryBackend) rebuildAsReplica(ctx context.Context, rec *instRec) error {
	done := make(chan error, 1)

	b.mu.Lock("rebuildAsReplica")
	b.enqueueLocked(rec, task{fn: func(tctx context.Context) error {
		done <- b.bringUp(tctx, rec)

		return nil
	}})
	b.mu.Unlock()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("waiting for the old primary to rejoin: %w", ctx.Err())
	}
}

// applyClusterLocked designates (or clears) the DR replica of a primary.
func (b *InMemoryBackend) applyClusterLocked(prim *instRec, in *ReplicationCluster) error {
	if in == nil {
		return nil
	}

	name := in.FailoverDrReplicaName

	if b.isReplica(prim) {
		if name != "" {
			return errInvalid("Only a primary instance can designate a DR replica.")
		}

		return nil
	}

	if name != "" && !slices.Contains(prim.Inst.ReplicaNames, name) {
		return errInvalid("%q is not a read replica of %s.", name, prim.Inst.Name)
	}

	if old := prim.Inst.ReplicationCluster; old != nil && old.FailoverDrReplicaName != "" {
		if r, ok := b.instances[instKey(prim.Inst.Project, old.FailoverDrReplicaName)]; ok {
			r.Inst.ReplicationCluster = nil
			b.touchLocked(r)
		}
	}

	if name == "" {
		prim.Inst.ReplicationCluster = nil

		return nil
	}

	prim.Inst.ReplicationCluster = &ReplicationCluster{FailoverDrReplicaName: name}

	if r, ok := b.instances[instKey(prim.Inst.Project, name)]; ok {
		r.Inst.ReplicationCluster = &ReplicationCluster{DrReplica: true}
		b.touchLocked(r)
	}

	return nil
}

// FailoverPromote promotes the DR replica of a primary (promoteReplica with failover=true) and rejoins the old
// primary as a replica of it.
func (b *InMemoryBackend) FailoverPromote(project, name string) (*Operation, error) {
	b.mu.Lock("FailoverPromote")
	defer b.mu.Unlock()

	rep, err := b.replicaOnly(project, name)
	if err != nil {
		return nil, err
	}

	prim, err := b.switchoverPrimaryLocked(rep)
	if err != nil {
		return nil, err
	}

	if c := prim.Inst.ReplicationCluster; c == nil || c.FailoverDrReplicaName != name {
		return nil, errInvalid("Replica failover requires the replica to be the DR replica of its primary.")
	}

	return b.submitLocked(rep, project, "PROMOTE_REPLICA", opMeta{}, func(ctx context.Context) error {
		return b.switchoverTask(ctx, rep)
	}), nil
}

// retireWriter deletes the engine a failover replaced, leaving the log archive its successor continues.
func retireWriter(ctx context.Context, old Engine) {
	if r, ok := old.(interface{ RetainArchive() }); ok {
		r.RetainArchive()
	}

	purgeEngine(ctx, old)
}
