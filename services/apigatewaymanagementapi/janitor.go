package apigatewaymanagementapi

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultJanitorInterval = time.Minute
	// defaultIdleTimeout matches API Gateway's documented 10-minute WebSocket idle timeout.
	defaultIdleTimeout   = 10 * time.Minute
	janitorWorkerService = "apigatewaymanagementapi"
	idleSweeperName      = "IdleConnectionSweeper"
)

// Janitor evicts WebSocket connections idle longer than IdleTimeout, so
// b.connections doesn't grow without bound.
type Janitor struct {
	Backend     StorageBackend
	Interval    time.Duration
	IdleTimeout time.Duration
	TaskTimeout time.Duration
}

// NewJanitor creates a new Janitor for the given backend. Zero values for
// interval or idleTimeout fall back to their defaults.
func NewJanitor(backend StorageBackend, interval, idleTimeout time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultJanitorInterval
	}

	if idleTimeout == 0 {
		idleTimeout = defaultIdleTimeout
	}

	return &Janitor{
		Backend:     backend,
		Interval:    interval,
		IdleTimeout: idleTimeout,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, janitorWorkerService)
	g.Ticker(idleSweeperName, j.Interval, j.TaskTimeout, j.sweepIdleConnections)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepIdleConnections(ctx)
}

func (j *Janitor) sweepIdleConnections(ctx context.Context) {
	pruned := j.Backend.PruneIdle(j.IdleTimeout)

	telemetry.RecordWorkerTask(janitorWorkerService, idleSweeperName, "success")

	if len(pruned) == 0 {
		return
	}

	telemetry.RecordWorkerItems(janitorWorkerService, idleSweeperName, len(pruned))
	logger.Load(ctx).InfoContext(ctx,
		"API Gateway Management API janitor: evicted idle connections",
		"evicted", len(pruned))
}
