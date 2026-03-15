package batch

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultBatchJanitorInterval   = time.Minute
	defaultBatchInactiveJobDefTTL = 24 * time.Hour

	batchWorkerServiceName         = "batch"
	inactiveJobDefSweeperComponent = "InactiveJobDefinitionSweeper"
)

// Janitor is the Batch background worker that evicts INACTIVE job definitions
// after a configurable TTL to prevent unbounded growth of in-memory state.
// This matches AWS behavior where deregistered definitions eventually disappear.
type Janitor struct {
	Backend           *InMemoryBackend
	Interval          time.Duration
	InactiveJobDefTTL time.Duration
}

// NewJanitor creates a new Batch Janitor for the given backend.
// Zero values for interval or inactiveJobDefTTL fall back to defaults.
func NewJanitor(backend *InMemoryBackend, interval, inactiveJobDefTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultBatchJanitorInterval
	}

	if inactiveJobDefTTL == 0 {
		inactiveJobDefTTL = defaultBatchInactiveJobDefTTL
	}

	return &Janitor{
		Backend:           backend,
		Interval:          interval,
		InactiveJobDefTTL: inactiveJobDefTTL,
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
			j.sweepInactiveJobDefinitions(ctx)
		}
	}
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepInactiveJobDefinitions(ctx)
}

// sweepInactiveJobDefinitions removes job definitions that have been in INACTIVE
// status for longer than InactiveJobDefTTL.
func (j *Janitor) sweepInactiveJobDefinitions(ctx context.Context) {
	cutoff := time.Now().Add(-j.InactiveJobDefTTL)

	j.Backend.mu.Lock("BatchJanitor")

	var swept []string

	for arnKey, jd := range j.Backend.jobDefinitions {
		if jd.Status == jobDefStatusInactive && jd.DeregisteredAt != nil && jd.DeregisteredAt.Before(cutoff) {
			swept = append(swept, arnKey)
			delete(j.Backend.jobDefinitions, arnKey)
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
