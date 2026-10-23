package rds

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	replStatusType        = "read replication"
	replStatusReplicating = "replicating"
	replStatusError       = "error"

	defaultReplicaPoll = 5 * time.Second
)

// DBInstanceStatusInfo is one entry of a DB instance's StatusInfos.
type DBInstanceStatusInfo struct {
	StatusType string
	Status     string
	Message    string
	Normal     bool
}

func (b *InMemoryBackend) replicaPollInterval() time.Duration {
	if d := b.engine.cfg.ReplicaPollInterval; d > 0 {
		return d
	}

	return defaultReplicaPoll
}

// startReplicaMonitor begins sampling a replica's replication health, once per unit.
func (b *InMemoryBackend) startReplicaMonitor(lb *liveDB) {
	b.mu.RLock("rdsReplicaMonitor")
	isReplica := lb.repl != nil
	interval := b.replicaPollInterval()
	b.mu.RUnlock()

	if !isReplica || !lb.monitoring.CompareAndSwap(false, true) {
		return
	}

	b.engine.wg.Go(func() {
		ctx, cancel := lb.opContext(0)
		defer cancel()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for b.pollReplica(ctx, lb) {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	})
}

// pollReplica samples the replica once; false means monitoring is over (promoted or dropped).
func (b *InMemoryBackend) pollReplica(ctx context.Context, lb *liveDB) bool {
	b.mu.RLock("rdsReplicaPoll")
	active := b.attachedLocked(lb) && lb.repl != nil && !lb.repl.promoting
	ready := active && lb.ready
	login, probe, id := b.engine.login(lb), b.engine.cfg.ReplicaStatus, lb.resourceID
	b.mu.RUnlock()

	if !ready {
		return active
	}

	pctx, cancel := context.WithTimeout(ctx, engineProbeTimeout)
	st, err := probe(pctx, login)

	cancel()

	if err != nil {
		st = ReplicaStatus{Message: err.Error()}
	}

	b.mu.Lock("rdsReplicaStatus")

	if b.attachedLocked(lb) && lb.repl != nil && !lb.repl.promoting {
		lb.repl.status, lb.repl.message, lb.repl.lag = replStatusError, st.Message, st.LagSeconds
		if st.Replicating {
			lb.repl.status, lb.repl.message = replStatusReplicating, ""
		}
	}

	b.mu.Unlock()

	b.metrics.Put(b.region, "AWS/RDS", "ReplicaLag", "Seconds", st.LagSeconds,
		cwmetric.Dimension{Name: "DBInstanceIdentifier", Value: id})

	return true
}

// overlayInstanceReplicaRLocked reports the replica's replication health on a describe copy.
func (b *InMemoryBackend) overlayInstanceReplicaRLocked(inst *DBInstance) {
	if inst.DBClusterIdentifier != "" || inst.ReplicaSourceDBInstanceIdentifier == "" {
		return
	}

	lb := b.standaloneUnitLocked(inst)
	if lb == nil || lb.repl == nil || !lb.ready || lb.repl.status == "" {
		return
	}

	inst.StatusInfos = []DBInstanceStatusInfo{{
		StatusType: replStatusType, Status: lb.repl.status, Message: lb.repl.message,
		Normal: lb.repl.status == replStatusReplicating,
	}}
}

// ReplicaLag returns the replica's last sampled replication lag; false when it is not a monitored replica.
func (b *InMemoryBackend) ReplicaLag(id string) (time.Duration, bool) {
	b.mu.RLock("ReplicaLag")
	defer b.mu.RUnlock()

	inst, ok := b.instances.Get(normalizeID(id))
	if !ok {
		return 0, false
	}

	lb := b.standaloneUnitLocked(inst)
	if lb == nil || lb.repl == nil || lb.repl.status == "" {
		return 0, false
	}

	return time.Duration(lb.repl.lag * float64(time.Second)), true
}

// promoteReplicaUnitLocked detaches a streaming replica's engine from its source in the background.
func (b *InMemoryBackend) promoteReplicaUnitLocked(inst *DBInstance) {
	lb := b.standaloneUnitLocked(inst)
	if lb == nil || lb.repl == nil || lb.repl.promoting {
		return
	}

	lb.repl.promoting = true

	if lb.ready {
		inst.DBInstanceStatus = instanceStatusModifying
	}

	b.engine.wg.Go(func() { b.finishPromote(lb) })
}

func (b *InMemoryBackend) finishPromote(lb *liveDB) {
	ctx, cancel := lb.opContext(0)
	defer cancel()

	_, in, err := b.awaitLiveSource(ctx, lb.key)
	if err == nil && b.stillReplica(lb) {
		err = b.engine.promoteEngine(ctx, in)
	}

	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "rds: promoting read replica failed", "code", "DB_ENGINE_PROMOTE_FAILED")
		b.failUnit(ctx, lb)

		return
	}

	if b.markPromoted(lb) {
		b.recordRestorePointForced(lb)
	}
}

// stillReplica is false for a replica that already started as a primary because its source vanished.
func (b *InMemoryBackend) stillReplica(lb *liveDB) bool {
	b.mu.RLock("rdsStillReplica")
	defer b.mu.RUnlock()

	return lb.repl != nil
}

func (b *InMemoryBackend) markPromoted(lb *liveDB) bool {
	b.mu.Lock("rdsEnginePromoted")
	defer b.mu.Unlock()

	if !b.attachedLocked(lb) {
		return false
	}

	lb.repl = nil
	b.unitStatusesLocked(lb, func(inst *DBInstance) {
		if inst.DBInstanceStatus == instanceStatusModifying {
			inst.DBInstanceStatus = instanceStatusAvailable
		}

		b.publishInstanceEventLocked(inst.DBInstanceIdentifier, "DB instance promoted to standalone")
	}, func(*DBCluster) {})

	return true
}

// promoteReplicasOfLocked promotes every replica of a source that is being deleted, as AWS does.
func (b *InMemoryBackend) promoteReplicasOfLocked(src *DBInstance) {
	for _, rid := range src.ReadReplicaIdentifiers {
		r, ok := b.instances.Get(normalizeID(rid))
		if !ok || !idEqual(r.ReplicaSourceDBInstanceIdentifier, src.DBInstanceIdentifier) {
			continue
		}

		r.ReplicaSourceDBInstanceIdentifier = ""
		b.promoteReplicaUnitLocked(r)
	}
}
