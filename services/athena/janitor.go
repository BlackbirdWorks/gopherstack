package athena

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultAthenaJanitorInterval = time.Minute
	defaultAthenaExecutionTTL    = 24 * time.Hour

	athenaWorkerServiceName   = "athena"
	executionSweeperComponent = "QueryExecutionSweeper"
)

// isTerminalExecution reports whether the given query execution state is terminal.
func isTerminalExecution(state string) bool {
	return state == "SUCCEEDED" || state == "FAILED" || state == "CANCELLED"
}

// Janitor is the Athena background worker that evicts completed query executions
// after a configurable TTL to prevent unbounded growth of in-memory state.
type Janitor struct {
	Backend      *InMemoryBackend
	Interval     time.Duration
	ExecutionTTL time.Duration
}

// NewJanitor creates a new Athena Janitor for the given backend.
// Zero values for interval or executionTTL fall back to defaults.
func NewJanitor(backend *InMemoryBackend, interval, executionTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultAthenaJanitorInterval
	}

	if executionTTL == 0 {
		executionTTL = defaultAthenaExecutionTTL
	}

	return &Janitor{
		Backend:      backend,
		Interval:     interval,
		ExecutionTTL: executionTTL,
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
			j.sweepCompletedExecutions(ctx)
		}
	}
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepCompletedExecutions(ctx)
}

// sweepCompletedExecutions removes query executions in terminal states whose
// CompletionDateTime is older than ExecutionTTL.
func (j *Janitor) sweepCompletedExecutions(ctx context.Context) {
	cutoff := float64(time.Now().Add(-j.ExecutionTTL).UnixMilli()) / millisToSeconds

	j.Backend.mu.Lock("AthenaJanitor")

	var swept []string

	for id, qe := range j.Backend.queryExecutions {
		if isTerminalExecution(qe.Status.State) &&
			qe.Status.CompletionDateTime > 0 &&
			qe.Status.CompletionDateTime < cutoff {
			swept = append(swept, id)
			delete(j.Backend.queryExecutions, id)
		}
	}

	j.Backend.mu.Unlock()

	count := len(swept)

	telemetry.RecordWorkerTask(athenaWorkerServiceName, executionSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(athenaWorkerServiceName, executionSweeperComponent, count)

	logger.Load(ctx).InfoContext(ctx, "Athena janitor: completed query executions evicted", "count", count)
}
