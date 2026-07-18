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
	g := worker.NewGroup(ctx, lambdaWorkerService)
	g.Ticker(runtimeJanitorName, j.Interval, j.TaskTimeout, j.sweep)

	<-ctx.Done()
	g.Stop()
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

	var toEvict []*functionRuntime

	func() {
		j.Backend.mu.Lock("JanitorSweepRuntimes")
		defer j.Backend.mu.Unlock()

		now := time.Now()

		for name, rt := range j.Backend.runtimes {
			if now.Sub(rt.lastUsed) > idleTimeout {
				toEvict = append(toEvict, rt)
				delete(j.Backend.runtimes, name)
			}
		}
	}()

	if len(toEvict) == 0 {
		telemetry.RecordWorkerTask(lambdaWorkerService, runtimeJanitorName, "success")

		return
	}

	for _, rt := range toEvict {
		j.Backend.cleanupRuntime(ctx, rt)
	}

	telemetry.RecordWorkerTask(lambdaWorkerService, runtimeJanitorName, "success")
	telemetry.RecordWorkerItems(lambdaWorkerService, runtimeJanitorName, len(toEvict))
	logger.Load(ctx).
		InfoContext(ctx, "Lambda janitor: evicted idle runtimes", "count", len(toEvict))
}

// esmHealthEntry is a snapshot of an ESM used for health checking outside the lock.
type esmHealthEntry struct {
	uuid        string
	functionARN string
}

// sweepESMs performs health checks on enabled event source mappings.
// For each enabled ESM whose function no longer exists, it marks the ESM
// LastProcessingResult as "PROBLEM" — mirroring the AWS behaviour where a
// deleted-function mapping transitions to a degraded state.
func (j *Janitor) sweepESMs(ctx context.Context) {
	var (
		esmCount int
		toCheck  []esmHealthEntry
	)

	func() {
		j.Backend.mu.RLock("JanitorSweepESMs")
		defer j.Backend.mu.RUnlock()

		esmCount = j.Backend.eventSourceMappings.Len()

		for _, esm := range j.Backend.eventSourceMappings.All() {
			if esm.State == ESMStateEnabled {
				toCheck = append(toCheck, esmHealthEntry{uuid: esm.UUID, functionARN: esm.FunctionARN})
			}
		}
	}()

	// Check each enabled ESM's function without holding the lock.
	var degraded []string
	for _, entry := range toCheck {
		fnName := functionNameFromARN(entry.functionARN)
		if _, err := j.Backend.GetFunction(fnName); err != nil {
			degraded = append(degraded, entry.uuid)
		}
	}

	if len(degraded) > 0 {
		func() {
			j.Backend.mu.Lock("JanitorSweepESMs.degrade")
			defer j.Backend.mu.Unlock()

			for _, id := range degraded {
				if esm, ok := j.Backend.eventSourceMappings.Get(id); ok {
					esm.LastProcessingResult = "PROBLEM"
				}
			}
		}()

		logger.Load(ctx).WarnContext(ctx, "Lambda janitor: ESMs with missing functions",
			"count", len(degraded))
		telemetry.RecordWorkerItems(lambdaWorkerService, esmJanitorName, len(degraded))
	}

	telemetry.RecordWorkerTask(lambdaWorkerService, esmJanitorName, "success")
	telemetry.RecordWorkerItems(lambdaWorkerService, esmJanitorName, esmCount)
}
