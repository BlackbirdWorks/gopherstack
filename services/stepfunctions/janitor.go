package stepfunctions

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	sfnWorkerService = "stepfunctions"
	prunerName       = "ExecutionPruner"
)

// Janitor is the Step Functions background worker that manages resource cleanup.
type Janitor struct {
	Backend     *InMemoryBackend
	Interval    time.Duration
	TaskTimeout time.Duration
}

// NewJanitor creates a new Janitor for the given backend.
func NewJanitor(backend *InMemoryBackend, settings Settings) *Janitor {
	interval := settings.JanitorInterval
	if interval == 0 {
		interval = defaultJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
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
			taskCtx, cancel := j.taskContext(ctx)
			j.sweepExecutions(taskCtx)
			cancel()
		}
	}
}

func (j *Janitor) taskContext(parent context.Context) (context.Context, context.CancelFunc) {
	if j.TaskTimeout > 0 {
		return context.WithTimeout(parent, j.TaskTimeout)
	}

	return context.WithCancel(parent)
}

func (j *Janitor) sweepExecutions(ctx context.Context) {
	pruned := j.Backend.PruneExecutions(ctx)
	evicted := j.Backend.SweepTaskTokens()

	telemetry.RecordWorkerTask(sfnWorkerService, prunerName, "success")
	if pruned > 0 {
		telemetry.RecordWorkerItems(sfnWorkerService, prunerName, pruned)
		logger.Load(ctx).InfoContext(ctx, "Step Functions janitor: pruned old executions", "count", pruned)
	}
	if evicted > 0 {
		telemetry.RecordWorkerItems(sfnWorkerService, prunerName, evicted)
		logger.Load(ctx).InfoContext(ctx, "Step Functions janitor: evicted stale task tokens", "count", evicted)
	}
}
