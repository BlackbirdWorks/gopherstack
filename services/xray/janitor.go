package xray

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultXRayJanitorInterval = time.Minute
	defaultXRayTraceTTL        = 30 * time.Minute

	xrayWorkerServiceName  = "xray"
	traceSweeperComponent  = "TraceAgeEvictor"
)

// Janitor is the X-Ray background worker that evicts old traces to prevent
// unbounded growth of in-memory state.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
	TraceTTL time.Duration
}

// NewJanitor creates a new X-Ray Janitor for the given backend.
// Zero values for interval or traceTTL fall back to defaults.
func NewJanitor(backend *InMemoryBackend, interval, traceTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultXRayJanitorInterval
	}

	if traceTTL == 0 {
		traceTTL = defaultXRayTraceTTL
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
		TraceTTL: traceTTL,
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
			j.sweepExpiredTraces(ctx)
		}
	}
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredTraces(ctx)
}

// sweepExpiredTraces removes traces older than TraceTTL.
func (j *Janitor) sweepExpiredTraces(ctx context.Context) {
	cutoff := time.Now().Add(-j.TraceTTL)

	j.Backend.mu.Lock("sweepExpiredTraces")

	var swept []string

	for id, t := range j.Backend.traces {
		if t.StartTime.Before(cutoff) {
			swept = append(swept, id)
			delete(j.Backend.traces, id)
		}
	}

	j.Backend.mu.Unlock()

	count := len(swept)

	telemetry.RecordWorkerTask(xrayWorkerServiceName, traceSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(xrayWorkerServiceName, traceSweeperComponent, count)

	logger.Load(ctx).InfoContext(ctx, "X-Ray janitor: expired traces evicted", "count", count)
}
