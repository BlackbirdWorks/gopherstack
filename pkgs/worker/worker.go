// Package worker provides a single leak-safe primitive for the background
// "janitor" loops that gopherstack services run: a ticker that invokes a sweep
// function on each tick until the context is cancelled. Centralising it removes
// the ~30 hand-rolled copies of the
//
//	ticker := time.NewTicker(interval); defer ticker.Stop()
//	for { select { case <-ctx.Done(): return; case <-ticker.C: sweep(ctx) } }
//
// loop (each a place a missing defer ticker.Stop() or ctx check could leak),
// applies the standard worker logging tags, and bounds each sweep with a
// timeout so a wedged sweep cannot stall shutdown.
package worker

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// RunTicker runs sweep once per interval until ctx is cancelled, then returns.
// The context passed to sweep is tagged with the worker's service and component
// (via [logger.WithWorker]) so all records emitted by the sweep are
// attributable. When timeout > 0 each sweep receives a context that is
// cancelled after timeout, bounding a single sweep's runtime; a timeout <= 0
// disables the per-sweep deadline. RunTicker owns the ticker and always stops
// it on return, so callers cannot leak it.
//
// Typical use from a service janitor:
//
//	func (j *Janitor) Run(ctx context.Context) {
//		worker.RunTicker(ctx, "sqs", "MessageRetentionSweeper", j.Interval, 0, j.sweep)
//	}
func RunTicker(
	ctx context.Context,
	service, component string,
	interval, timeout time.Duration,
	sweep func(context.Context),
) {
	ctx = logger.WithWorker(ctx, service, component)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			runSweep(ctx, timeout, sweep)
		}
	}
}

// runSweep invokes sweep, optionally under a per-sweep timeout context.
func runSweep(ctx context.Context, timeout time.Duration, sweep func(context.Context)) {
	if timeout <= 0 {
		sweep(ctx)

		return
	}

	sweepCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	sweep(sweepCtx)
}
