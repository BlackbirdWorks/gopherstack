package fis

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultFISJanitorInterval = time.Minute
	defaultFISExperimentTTL   = 24 * time.Hour

	fisWorkerServiceName       = "fis"
	experimentSweeperComponent = "CompletedExperimentSweeper"
)

// isTerminalExperiment reports whether the given experiment status is terminal.
func isTerminalExperiment(status string) bool {
	return status == statusCompleted || status == statusStopped || status == statusFailed
}

// Janitor is the FIS background worker that evicts completed experiments
// after a configurable TTL to prevent unbounded growth of in-memory state.
type Janitor struct {
	Backend       *InMemoryBackend
	Interval      time.Duration
	ExperimentTTL time.Duration
}

// NewJanitor creates a new FIS Janitor for the given backend.
// Zero values for interval or experimentTTL fall back to defaults.
func NewJanitor(backend *InMemoryBackend, interval, experimentTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultFISJanitorInterval
	}

	if experimentTTL == 0 {
		experimentTTL = defaultFISExperimentTTL
	}

	return &Janitor{
		Backend:       backend,
		Interval:      interval,
		ExperimentTTL: experimentTTL,
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
			j.sweepCompletedExperiments(ctx)
		}
	}
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepCompletedExperiments(ctx)
}

// sweepCompletedExperiments removes experiments in terminal states whose EndTime
// is older than ExperimentTTL.
func (j *Janitor) sweepCompletedExperiments(ctx context.Context) {
	cutoff := time.Now().Add(-j.ExperimentTTL)

	j.Backend.mu.Lock()

	var swept []string

	for id, exp := range j.Backend.experiments {
		if isTerminalExperiment(exp.Status.Status) && exp.EndTime != nil && exp.EndTime.Before(cutoff) {
			swept = append(swept, id)
			delete(j.Backend.experiments, id)
		}
	}

	j.Backend.mu.Unlock()

	count := len(swept)

	telemetry.RecordWorkerTask(fisWorkerServiceName, experimentSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(fisWorkerServiceName, experimentSweeperComponent, count)

	logger.Load(ctx).InfoContext(ctx, "FIS janitor: completed experiments evicted", "count", count)
}
