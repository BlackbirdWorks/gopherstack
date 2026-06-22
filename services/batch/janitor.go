package batch

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultBatchJanitorInterval   = time.Minute
	defaultBatchInactiveJobDefTTL = 24 * time.Hour
	defaultBatchCompletedJobTTL   = 24 * time.Hour

	batchWorkerServiceName         = "batch"
	inactiveJobDefSweeperComponent = "InactiveJobDefinitionSweeper"
	completedJobSweeperComponent   = "CompletedJobSweeper"
)

// Janitor is the Batch background worker that evicts INACTIVE job definitions
// after a configurable TTL to prevent unbounded growth of in-memory state.
// This matches AWS behavior where deregistered definitions eventually disappear.
// It also evicts completed and failed jobs after a configurable TTL, matching
// the AWS Batch job history retention behavior.
type Janitor struct {
	Backend           *InMemoryBackend
	Interval          time.Duration
	InactiveJobDefTTL time.Duration
	CompletedJobTTL   time.Duration
	TaskTimeout       time.Duration
}

// NewJanitor creates a new Batch Janitor for the given backend.
// Zero values for interval, inactiveJobDefTTL, or completedJobTTL fall back to defaults.
func NewJanitor(backend *InMemoryBackend, interval, inactiveJobDefTTL, completedJobTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultBatchJanitorInterval
	}

	if inactiveJobDefTTL == 0 {
		inactiveJobDefTTL = defaultBatchInactiveJobDefTTL
	}

	if completedJobTTL == 0 {
		completedJobTTL = defaultBatchCompletedJobTTL
	}

	return &Janitor{
		Backend:           backend,
		Interval:          interval,
		InactiveJobDefTTL: inactiveJobDefTTL,
		CompletedJobTTL:   completedJobTTL,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	worker.RunTicker(
		ctx,
		batchWorkerServiceName,
		inactiveJobDefSweeperComponent,
		j.Interval,
		j.TaskTimeout,
		j.SweepOnce,
	)
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepInactiveJobDefinitions(ctx)
	j.sweepCompletedJobs(ctx)
}

// sweepInactiveJobDefinitions removes job definitions that have been in INACTIVE
// status for longer than InactiveJobDefTTL. Orphaned revision counters (names
// with no remaining definitions) are also removed to prevent unbounded growth.
func (j *Janitor) sweepInactiveJobDefinitions(ctx context.Context) {
	cutoff := time.Now().Add(-j.InactiveJobDefTTL)

	j.Backend.mu.Lock("BatchJanitor")

	var swept []string

	// Job definitions are nested by region; sweep each region independently so
	// expired INACTIVE definitions and orphaned revision counters are cleaned up
	// per region.
	for region, defs := range j.Backend.jobDefinitions {
		for arnKey, jd := range defs {
			if jd.Status == jobDefStatusInactive && jd.DeregisteredAt != nil && jd.DeregisteredAt.Before(cutoff) {
				swept = append(swept, arnKey)
				delete(defs, arnKey)
			}
		}

		// Remove revision counters for names that no longer have any definition
		// (ACTIVE or INACTIVE) in this region. This prevents the jobDefRevisions
		// map from growing without bound as job definition names cycle through
		// their lifetimes. Build a set of surviving names first for O(n+m).
		surviving := make(map[string]struct{}, len(defs))

		for _, jd := range defs {
			surviving[jd.JobDefinitionName] = struct{}{}
		}

		revisions := j.Backend.jobDefRevisions[region]
		for name := range revisions {
			if _, ok := surviving[name]; !ok {
				delete(revisions, name)
			}
		}
	}

	j.Backend.mu.Unlock()

	count := len(swept)

	telemetry.RecordWorkerTask(batchWorkerServiceName, inactiveJobDefSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(batchWorkerServiceName, inactiveJobDefSweeperComponent, count)

	logger.Load(ctx).InfoContext(ctx, "Batch janitor: INACTIVE job definitions evicted", "count", count)
}

// sweepCompletedJobs removes completed or failed Batch jobs whose StoppedAt
// timestamp is older than CompletedJobTTL. This mirrors AWS Batch behavior where
// job history is retained for a limited period before automatic removal.
func (j *Janitor) sweepCompletedJobs(ctx context.Context) {
	cutoffMs := time.Now().Add(-j.CompletedJobTTL).UnixMilli()

	j.Backend.mu.Lock("BatchJanitorCompletedJobs")

	var swept []string

	// Jobs are nested by region; sweep completed/failed jobs in every region.
	for region, jobs := range j.Backend.jobs {
		for id, job := range jobs {
			if !isTerminalJobStatus(job.Status) {
				continue
			}

			if job.StoppedAt == nil {
				continue
			}

			if *job.StoppedAt < cutoffMs {
				swept = append(swept, id)
				delete(jobs, id)

				if jobsByARN := j.Backend.jobsByARN[region]; jobsByARN != nil {
					delete(jobsByARN, job.JobARN)
				}
			}
		}
	}

	j.Backend.mu.Unlock()

	count := len(swept)

	telemetry.RecordWorkerTask(batchWorkerServiceName, completedJobSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(batchWorkerServiceName, completedJobSweeperComponent, count)

	logger.Load(ctx).InfoContext(ctx, "Batch janitor: completed jobs evicted", "count", count)
}

// isTerminalJobStatus reports whether the given job status is terminal.
func isTerminalJobStatus(status string) bool {
	return status == jobStatusSucceeded || status == jobStatusFailed
}
