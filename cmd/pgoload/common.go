package main

import (
	"context"
	"log/slog"
	"sync/atomic"
)

// opCounter tracks operation and error totals for one breadth-load scenario.
type opCounter struct {
	ops    atomic.Int64
	errors atomic.Int64
}

func (o *opCounter) record(err error) {
	if err != nil {
		o.errors.Add(1)

		return
	}

	o.ops.Add(1)
}

// opFunc is one operation exercised by a breadth-scenario worker. workerID
// and i together select the resource/key space touched by the call.
type opFunc func(ctx context.Context, workerID, i int) error

// runOpLoop repeatedly runs a mix of operations, staggered by workerID so
// concurrent workers hit different operations on each tick, until ctx is
// done. It is the shared loop shape behind every breadth scenario added
// beyond the original DDB/S3 load (ddbWorker and s3Worker keep their own
// copy of this loop untouched).
func runOpLoop(ctx context.Context, workerID int, ops []opFunc, c *opCounter, scenario string, log *slog.Logger) {
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := ops[(workerID+i)%len(ops)](ctx, workerID, i)
		c.record(err)

		if err != nil {
			log.WarnContext(ctx, scenario+" operation failed", "worker", workerID, "iter", i, "error", err)
		}
	}
}
