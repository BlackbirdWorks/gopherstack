package redshiftdata

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultJanitorInterval    = time.Minute
	defaultStatementTTL       = 24 * time.Hour
	redshiftDataWorkerService = "redshift-data"
	statementSweeperComponent = "StatementSweeper"
)

// Janitor is the Redshift Data background worker that evicts completed statements
// after a configurable TTL to prevent unbounded growth of in-memory state.
// It complements the ring-buffer cap: the ring buffer evicts by count,
// the janitor evicts by age.
type Janitor struct {
	Backend      *InMemoryBackend
	Interval     time.Duration
	StatementTTL time.Duration
	// TaskTimeout bounds each individual sweep task. When zero, each task runs
	// without a deadline. When non-zero, a child context with this timeout is
	// created for each sweep pass, preventing a stalled operation from blocking
	// the janitor loop indefinitely.
	TaskTimeout time.Duration
}

// NewJanitor creates a new Janitor for the given backend.
// Zero values for interval or statementTTL fall back to package defaults.
func NewJanitor(backend *InMemoryBackend, interval, statementTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultJanitorInterval
	}

	if statementTTL == 0 {
		statementTTL = defaultStatementTTL
	}

	return &Janitor{
		Backend:      backend,
		Interval:     interval,
		StatementTTL: statementTTL,
	}
}

// Run runs the janitor loop until ctx is cancelled.
// It should be started in a goroutine: go janitor.Run(ctx).
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, redshiftDataWorkerService)
	g.Ticker(statementSweeperComponent, j.Interval, j.TaskTimeout, j.SweepOnce)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	cutoff := time.Now().Add(-j.StatementTTL)
	count := j.Backend.EvictExpiredStatements(cutoff)

	telemetry.RecordWorkerTask(redshiftDataWorkerService, statementSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(redshiftDataWorkerService, statementSweeperComponent, count)

	logger.Load(ctx).InfoContext(ctx, "redshiftdata: janitor evicted statements", "count", count)
}
