package xray

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultXRayJanitorInterval = time.Minute
	defaultXRayTraceTTL        = 30 * time.Minute

	xrayWorkerServiceName = "xray"
	traceSweeperComponent = "TraceAgeEvictor"
)

// Janitor is the X-Ray background worker that evicts old traces to prevent
// unbounded growth of in-memory state.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
	TraceTTL time.Duration
	// TaskTimeout bounds each individual janitor task. When non-zero, each task
	// runs with a child context that expires after this duration, preventing a
	// stalled operation from blocking the janitor loop indefinitely.
	TaskTimeout time.Duration
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
	g := worker.NewGroup(ctx, xrayWorkerServiceName)
	g.Ticker(
		traceSweeperComponent,
		j.Interval,
		j.TaskTimeout,
		j.sweepExpiredTraces,
	)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredTraces(ctx)
}

// sweepExpiredTraces removes traces older than TraceTTL.
// It also cleans up the associated parsed-segment indexes to prevent memory leaks.
func (j *Janitor) sweepExpiredTraces(ctx context.Context) {
	cutoff := time.Now().Add(-j.TraceTTL)

	j.Backend.mu.Lock("sweepExpiredTraces")

	var swept []string

	for id, t := range j.Backend.traces {
		if t.StartTime.Before(cutoff) {
			swept = append(swept, id)
			delete(j.Backend.traces, id)

			// Clean up segment indexes for the evicted trace.
			if segs, ok := j.Backend.traceSegments[id]; ok {
				for _, seg := range segs {
					delete(j.Backend.parsedSegments, id+":"+seg.ID)
				}

				delete(j.Backend.traceSegments, id)
			}
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
