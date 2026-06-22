package lambda

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultJanitorInterval = 1 * time.Minute
	lambdaWorkerService    = "lambda"
	runtimeJanitorName     = "RuntimeJanitor"
	esmJanitorName         = "ESMJanitor"
)

// Janitor is the Lambda background worker that manages resource cleanup.
// It evicts idle runtimes (containers) and cleans up stale event source mappings.
type Janitor struct {
	Backend     *InMemoryBackend
	Interval    time.Duration
	TaskTimeout time.Duration
}

// NewJanitor creates a new Lambda Janitor for the given backend.
func NewJanitor(backend *InMemoryBackend, _ Settings) *Janitor {
	return &Janitor{
		Backend:  backend,
		Interval: defaultJanitorInterval,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	worker.RunTicker(ctx, lambdaWorkerService, runtimeJanitorName, j.Interval, j.TaskTimeout, j.sweep)
}

// sweep runs one full janitor pass: evict idle runtimes and health-check ESMs.
func (j *Janitor) sweep(ctx context.Context) {
	j.sweepIdleRuntimes(ctx)
	j.sweepESMs(ctx)
}

// sweepIdleRuntimes identifies runtimes that have been idle for longer than
// the configured IdleTimeout and shuts them down.
func (j *Janitor) sweepIdleRuntimes(ctx context.Context) {
	idleTimeout := j.Backend.settings.IdleTimeout
	if idleTimeout == 0 {
		idleTimeout = defaultIdleTimeout
	}

	j.Backend.mu.Lock("JanitorSweepRuntimes")
	var toEvict []*functionRuntime
	now := time.Now()

	for name, rt := range j.Backend.runtimes {
		if now.Sub(rt.lastUsed) > idleTimeout {
			toEvict = append(toEvict, rt)
			delete(j.Backend.runtimes, name)
		}
	}
	j.Backend.mu.Unlock()

	if len(toEvict) == 0 {
		telemetry.RecordWorkerTask(lambdaWorkerService, runtimeJanitorName, "success")

		return
	}

	for _, rt := range toEvict {
		j.Backend.cleanupRuntime(ctx, rt)
	}

	telemetry.RecordWorkerTask(lambdaWorkerService, runtimeJanitorName, "success")
	telemetry.RecordWorkerItems(lambdaWorkerService, runtimeJanitorName, len(toEvict))
	logger.Load(ctx).InfoContext(ctx, "Lambda janitor: evicted idle runtimes", "count", len(toEvict))
}

// sweepESMs performs health checks on active event source mappings.
// Currently it just records metrics.
func (j *Janitor) sweepESMs(_ context.Context) {
	j.Backend.mu.RLock("JanitorSweepESMs")
	esmCount := len(j.Backend.eventSourceMappings)
	j.Backend.mu.RUnlock()

	telemetry.RecordWorkerTask(lambdaWorkerService, esmJanitorName, "success")
	telemetry.RecordWorkerItems(lambdaWorkerService, esmJanitorName, esmCount)
}
