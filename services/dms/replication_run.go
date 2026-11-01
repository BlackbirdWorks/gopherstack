package dms

import (
	"context"
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/dmsengine"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// replicationEligible reports whether a replication config's endpoints are ones the engine can run.
func (b *InMemoryBackend) replicationEndpoints(region string, rc *ReplicationConfig) (*Endpoint, *Endpoint, bool) {
	src, ok1 := lookupUnique(b.endpointsByARN, regionKey(region, rc.SourceEndpointArn))
	tgt, ok2 := lookupUnique(b.endpointsByARN, regionKey(region, rc.TargetEndpointArn))

	return src, tgt, ok1 && ok2 && b.engineEligible(src, tgt)
}

// launchReplicationLocked starts the engine for a Serverless replication; the caller holds b.mu and has set the
// status to starting.
func (b *InMemoryBackend) launchReplicationLocked(
	region string, rc *ReplicationConfig, src, tgt *Endpoint, startType string,
) {
	p := runParams{
		taskARN: rc.ReplicationConfigArn, region: region, slot: slotName(rc.ReplicationConfigArn),
		migration: rc.ReplicationType, mappings: rc.TableMappings, settings: rc.ReplicationSettings,
		cdcStart: rc.CdcStartPosition, cdcStop: rc.CdcStopPosition,
		source: b.endpointConfigLocked(region, src), target: b.endpointConfigLocked(region, tgt),
		sourceEngine: src.EngineName, targetEngine: tgt.EngineName, replication: true,
	}

	if p.cdcStart == "" && rc.CdcStartTime != nil {
		p.cdcStart = rc.CdcStartTime.UTC().Format(time.RFC3339)
	}

	if startType == startTypeResume {
		p.resume = rc.RecoveryCheckpoint
	}

	b.launchRunLocked(p)
}

func (b *InMemoryBackend) beginReplicationLocked(rc *ReplicationConfig, startType string) {
	now := b.clock()
	rc.Status, rc.StartDate, rc.StopDate, rc.UpdateTime = statusStarting, now, time.Time{}, now
	rc.StopReason, rc.FailureMessage = "", ""

	if startType != startTypeResume {
		rc.TableStats, rc.RecoveryCheckpoint = nil, ""
		rc.FullLoadStartDate, rc.FullLoadFinishDate = time.Time{}, time.Time{}
	}
}

func (b *InMemoryBackend) recordReplicationProgress(p runParams, pr dmsengine.Progress) {
	rc, ok := lookupUnique(b.replicationConfigsByARN, regionKey(p.region, p.taskARN))
	if !ok {
		return
	}

	if pr.Running && rc.Status == statusStarting {
		rc.Status = statusRunning
	}

	rc.TableStats = statsFrom(pr)
	rc.FullLoadStartDate, rc.FullLoadFinishDate = pr.FullLoadStart, pr.FullLoadFinish
	rc.UpdateTime = b.clock()

	if pr.Checkpoint != "" {
		rc.RecoveryCheckpoint = pr.Checkpoint
	}
}

func (b *InMemoryBackend) finishReplication(ctx context.Context, p runParams, res dmsengine.Result) {
	rc, ok := lookupUnique(b.replicationConfigsByARN, regionKey(p.region, p.taskARN))
	if !ok {
		return
	}

	rc.StopDate, rc.StopReason, rc.UpdateTime = b.clock(), res.Reason, b.clock()

	if res.Err != nil {
		rc.Status, rc.FailureMessage = statusFailed, res.Err.Error()
		logger.Load(ctx).
			WarnContext(ctx, "dms: serverless replication failed", "config", rc.ReplicationConfigIdentifier,
				"error", res.Err)

		return
	}

	rc.Status = statusStopped
}

// viewReplication copies rc for a response and derives ElapsedTimeMillis.
func (b *InMemoryBackend) viewReplication(rc *ReplicationConfig) *ReplicationConfig {
	cp := *rc

	switch {
	case rc.StartDate.IsZero():
	case rc.StopDate.IsZero():
		cp.ElapsedTimeMillis = b.clock().Sub(rc.StartDate).Milliseconds()
	default:
		cp.ElapsedTimeMillis = rc.StopDate.Sub(rc.StartDate).Milliseconds()
	}

	return &cp
}

func replicationActive(status string) bool {
	return status == statusRunning || status == statusStarting || status == statusStopping
}

// reloadChecked re-copies tables on a live run once every table is known to it; no run means nothing to reload.
func (b *InMemoryBackend) reloadChecked(run *taskRun, owner string, refs []dmsengine.TableRef) error {
	if run == nil || run.task == nil {
		return nil
	}

	for _, ref := range refs {
		if !run.task.Has(ref) {
			return fmt.Errorf("%w: table %s.%s is not part of %s", ErrNotFound, ref.Schema, ref.Name, owner)
		}
	}

	b.reloadRun(run, run.task, refs)

	return nil
}
