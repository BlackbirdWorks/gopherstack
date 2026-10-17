package backup

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const windowHalves = 2

// SetLifecycleDelay sets how long restore and copy jobs stay in their transitional states before they
// settle; zero (the default) settles them synchronously. Jobs settle lazily on the next read.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()

	b.lifecycleDelay = d
}

// settleDueJobsLocked advances every delayed restore and copy job to its state at now.
func (b *InMemoryBackend) settleDueJobsLocked(now time.Time) {
	for _, j := range b.restoreJobs.All() {
		if j.SettleAt == nil {
			continue
		}
		if !now.Before(*j.SettleAt) {
			b.settleRestoreJobLocked(j, now)

			continue
		}
		j.Status = transitionalState(j.StartTime, *j.SettleAt, now, statusPending)
	}

	for _, j := range b.copyJobs.All() {
		if j.SettleAt == nil {
			continue
		}
		if !now.Before(*j.SettleAt) {
			b.settleCopyJobLocked(j, now)

			continue
		}
		j.State = transitionalState(j.CreationDate, *j.SettleAt, now, statusCreated)
	}
}

// transitionalState is first for the opening half of the window and RUNNING for the rest.
func transitionalState(start, settleAt, now time.Time, first string) string {
	if now.Before(start.Add(settleAt.Sub(start) / windowHalves)) {
		return first
	}

	return statusRunning
}

// settleRestoreJobLocked materializes the restored resource and finishes the job as COMPLETED or FAILED.
func (b *InMemoryBackend) settleRestoreJobLocked(job *RestoreJob, now time.Time) {
	job.SettleAt = nil
	job.CompletionDate = &now

	created, err := b.restoreResourceLocked(job)
	if err != nil {
		job.Status = statusFailed
		job.StatusMessage = err.Error()

		return
	}

	job.Status = statusCompleted
	job.PercentDone = "100.0"
	job.CreatedResourceArn = created
}

// restoreResourceLocked recreates the resource through the wired engine for the job's resource type. A
// recovery point with no captured data (untracked ARN, unwired service) keeps the synthesized ARN.
func (b *InMemoryBackend) restoreResourceLocked(job *RestoreJob) (string, error) {
	rp, found := b.findRecoveryPointByArn(job.RecoveryPointArn)
	engine := b.engines[job.ResourceType]

	if found && len(rp.Snapshot) > 0 && engine != nil {
		created, err := engine.restore(context.Background(), rp.Snapshot, job.Metadata)
		if err != nil {
			return "", fmt.Errorf("restore %s: %w", job.ResourceType, err)
		}

		return created, nil
	}

	if job.ResourceType == "" {
		return "", nil
	}

	return "arn:aws:" + strings.ToLower(job.ResourceType) + ":" + b.region + ":" + b.accountID +
		":restored/" + job.RestoreJobID, nil
}

// captureSnapshotLocked snapshots the backed-up resource when an engine is wired for its type.
func (b *InMemoryBackend) captureSnapshotLocked(job *Job) (json.RawMessage, error) {
	engine := b.engines[job.ResourceType]
	if engine == nil {
		return nil, nil
	}

	data, err := engine.capture(context.Background(), job.ResourceArn)
	if err != nil {
		return nil, fmt.Errorf("snapshot %s: %w", job.ResourceArn, err)
	}

	return data, nil
}
