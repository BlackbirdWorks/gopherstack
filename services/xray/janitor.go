package xray

import (
	"context"
	"slices"
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

	for _, t := range j.Backend.traces.All() {
		if t.StartTime.Before(cutoff) {
			swept = append(swept, t.TraceID)
		}
	}

	for _, id := range swept {
		j.Backend.traces.Delete(id)

		// Clean up segment indexes for the evicted trace. Clone the index
		// result first: parsedSegments.Delete below mutates the traceSegments
		// index in place, which would otherwise corrupt this same slice
		// while it is still being ranged over.
		segs := slices.Clone(j.Backend.traceSegments.Get(id))
		for _, seg := range segs {
			j.Backend.parsedSegments.Delete(seg.TraceID + ":" + seg.ID)
		}
	}

	// Sweep expired retrieval tokens.
	for token, created := range j.Backend.retrievalTimes {
		if created.Before(cutoff) {
			j.Backend.traceRetrievals.Delete(token)
			delete(j.Backend.retrievedTraces, token)
			delete(j.Backend.retrievalTimes, token)
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
