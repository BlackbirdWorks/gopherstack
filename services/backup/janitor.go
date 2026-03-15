package backup

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultBackupJanitorInterval = time.Minute
	defaultBackupJobTTL          = 24 * time.Hour

	backupWorkerServiceName = "backup"
	jobSweeperComponent     = "CompletedJobSweeper"
)

// isTerminalJob reports whether the given backup job state is terminal.
func isTerminalJob(state string) bool {
	return state == "COMPLETED" || state == "FAILED" || state == "EXPIRED" || state == "ABORTED"
}

// Janitor is the Backup background worker that evicts completed backup jobs
// after a configurable TTL to prevent unbounded growth of in-memory state.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
	JobTTL   time.Duration
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
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.sweepCompletedJobs(ctx)
		}
	}
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepCompletedJobs(ctx)
}

// sweepCompletedJobs removes backup jobs in terminal states whose CompletionTime
// is older than JobTTL.
func (j *Janitor) sweepCompletedJobs(ctx context.Context) {
	cutoff := time.Now().Add(-j.JobTTL)

	j.Backend.mu.Lock("BackupJanitor")

	var swept []string

	for id, job := range j.Backend.jobs {
		if isTerminalJob(job.State) && job.CompletionTime != nil && job.CompletionTime.Before(cutoff) {
			swept = append(swept, id)
			delete(j.Backend.jobs, id)
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
