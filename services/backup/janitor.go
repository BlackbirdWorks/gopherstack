package backup

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultBackupJanitorInterval = time.Minute
	defaultBackupJobTTL          = 24 * time.Hour

	backupWorkerServiceName            = "backup"
	jobSweeperComponent                = "CompletedJobSweeper"
	jobAdvanceComponent                = "CreatedJobAdvancer"
	restoreAccessVaultAdvanceComponent = "RestoreAccessVaultAdvancer"
	backupAccessPointAdvanceComponent  = "BackupAccessPointAdvancer"
)

// isTerminalJob reports whether the given backup job state is terminal.
func isTerminalJob(state string) bool {
	return state == statusCompleted || state == statusFailed || state == "EXPIRED" || state == "ABORTED"
}

// Janitor is the Backup background worker that evicts completed backup jobs
// after a configurable TTL to prevent unbounded growth of in-memory state.
type Janitor struct {
	Backend     *InMemoryBackend
	Interval    time.Duration
	JobTTL      time.Duration
	TaskTimeout time.Duration
}

// NewJanitor creates a new Backup Janitor for the given backend.
// Zero values for interval or jobTTL fall back to defaults.
func NewJanitor(backend *InMemoryBackend, interval, jobTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultBackupJanitorInterval
	}

	if jobTTL == 0 {
		jobTTL = defaultBackupJobTTL
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
		JobTTL:   jobTTL,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, backupWorkerServiceName)
	g.Ticker(
		jobSweeperComponent,
		j.Interval,
		j.TaskTimeout,
		j.SweepOnce,
	)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.advanceCreatedJobs(ctx)
	j.advanceRestoreAccessVaults(ctx)
	j.advanceBackupAccessPoints(ctx)
	j.sweepCompletedJobs(ctx)
}

// advanceCreatedJobs moves CREATED backup jobs to RUNNING and completes the jobs that were already
// RUNNING at the start of the sweep, so a job is observable in each state for one janitor interval.
func (j *Janitor) advanceCreatedJobs(ctx context.Context) {
	var toRun, toComplete []string

	j.Backend.mu.RLock("BackupJanitorAdvanceJobsLock")
	for _, job := range j.Backend.jobs.All() {
		switch job.State {
		case statusCreated:
			toRun = append(toRun, job.BackupJobID)
		case statusRunning:
			toComplete = append(toComplete, job.BackupJobID)
		}
	}
	j.Backend.mu.RUnlock()

	if len(toRun)+len(toComplete) == 0 {
		return
	}

	for _, id := range toComplete {
		_ = j.Backend.CompleteBackupJob(id)
	}

	for _, id := range toRun {
		_ = j.Backend.RunBackupJob(id)
	}

	telemetry.RecordWorkerTask(backupWorkerServiceName, jobAdvanceComponent, "success")
	telemetry.RecordWorkerItems(backupWorkerServiceName, jobAdvanceComponent, len(toRun)+len(toComplete))

	logger.Load(ctx).InfoContext(
		ctx, "Backup janitor: backup jobs advanced", "running", len(toRun), "completed", len(toComplete),
	)
}

// advanceRestoreAccessVaults completes restore access backup vaults still in
// the CREATING state, moving them to AVAILABLE. CreateRestoreAccessBackupVault
// stamped VaultState CREATING and nothing else in this backend ever advanced
// it -- no ticker, no later call -- so ListRestoreAccessBackupVaults showed
// CREATING for the entire lifetime of every restore access vault ever
// created. CreateRestoreAccessBackupVault itself stays synchronous so callers
// immediately observe CREATING, matching AWS's own response, the same
// StartBackupJob-shaped rationale as advanceCreatedJobs above.
func (j *Janitor) advanceRestoreAccessVaults(ctx context.Context) {
	var toAdvance []string

	j.Backend.mu.RLock("BackupJanitorAdvanceRestoreAccessVaultsLock")
	for _, v := range j.Backend.restoreAccessVaults.All() {
		if v.VaultState == statusCreating {
			toAdvance = append(toAdvance, v.RestoreAccessBackupVaultName)
		}
	}
	j.Backend.mu.RUnlock()

	if len(toAdvance) == 0 {
		return
	}

	j.Backend.mu.Lock("BackupJanitorAdvanceRestoreAccessVaultsLock")
	for _, name := range toAdvance {
		if v, ok := j.Backend.restoreAccessVaults.Get(name); ok && v.VaultState == statusCreating {
			v.VaultState = statusAvailable
		}
	}
	j.Backend.mu.Unlock()

	telemetry.RecordWorkerTask(backupWorkerServiceName, restoreAccessVaultAdvanceComponent, "success")
	telemetry.RecordWorkerItems(backupWorkerServiceName, restoreAccessVaultAdvanceComponent, len(toAdvance))

	logger.Load(ctx).InfoContext(
		ctx, "Backup janitor: restore access vaults became available", "count", len(toAdvance),
	)
}

// advanceBackupAccessPoints completes backup access points still in the
// CREATING state, moving them to AVAILABLE and populating the
// S3AccessPointArn/S3AccessPointAlias keys real AWS documents appearing in
// AccessPointMetadata once available (see s3AccessPointArnFor). Same
// synchronous-create-then-janitor-advances shape as advanceRestoreAccessVaults.
func (j *Janitor) advanceBackupAccessPoints(ctx context.Context) {
	var toAdvance []string

	j.Backend.mu.RLock("BackupJanitorAdvanceBackupAccessPointsLock")
	for _, bap := range j.Backend.backupAccessPoints.All() {
		if bap.Status == statusCreating {
			toAdvance = append(toAdvance, bap.AccessPointArn)
		}
	}
	j.Backend.mu.RUnlock()

	if len(toAdvance) == 0 {
		return
	}

	j.Backend.mu.Lock("BackupJanitorAdvanceBackupAccessPointsLock")
	for _, accessPointArn := range toAdvance {
		bap, ok := j.Backend.backupAccessPoints.Get(accessPointArn)
		if !ok || bap.Status != statusCreating {
			continue
		}

		bap.Status = statusAvailable
		if bap.AccessPointMetadata == nil {
			bap.AccessPointMetadata = map[string]string{}
		}

		bap.AccessPointMetadata["S3AccessPointArn"] = s3AccessPointArnFor(
			j.Backend.region, j.Backend.accountID, bap.Name,
		)
		bap.AccessPointMetadata["S3AccessPointAlias"] = s3AccessPointAliasFor(
			j.Backend.accountID, bap.Name,
		)
	}
	j.Backend.mu.Unlock()

	telemetry.RecordWorkerTask(backupWorkerServiceName, backupAccessPointAdvanceComponent, "success")
	telemetry.RecordWorkerItems(backupWorkerServiceName, backupAccessPointAdvanceComponent, len(toAdvance))

	logger.Load(ctx).InfoContext(
		ctx, "Backup janitor: backup access points became available", "count", len(toAdvance),
	)
}

// sweepCompletedJobs removes backup jobs in terminal states whose CompletionTime
// is older than JobTTL.
func (j *Janitor) sweepCompletedJobs(ctx context.Context) {
	cutoff := time.Now().Add(-j.JobTTL)

	j.Backend.mu.Lock("BackupJanitor")

	var swept []string

	for _, job := range j.Backend.jobs.All() {
		if isTerminalJob(job.State) && job.CompletionTime != nil && job.CompletionTime.Before(cutoff) {
			swept = append(swept, job.BackupJobID)
			j.Backend.jobs.Delete(job.BackupJobID)
		}
	}

	j.Backend.mu.Unlock()

	count := len(swept)

	telemetry.RecordWorkerTask(backupWorkerServiceName, jobSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(backupWorkerServiceName, jobSweeperComponent, count)

	logger.Load(ctx).InfoContext(ctx, "Backup janitor: completed jobs evicted", "count", count)
}
