package sqs

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultSQSJanitorInterval = time.Minute
	sqsJanitorService         = "sqs"
	sqsJanitorComponent       = "MessageRetentionSweeper"
)

// Janitor is the SQS background worker that deletes messages that have exceeded
// their MessageRetentionPeriod.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new SQS Janitor for the given backend.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultSQSJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
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
			j.sweepExpiredMessages(ctx)
		}
	}
}

// sweepExpiredMessages removes messages that have exceeded the queue's MessageRetentionPeriod
// and runs all standard backend maintenance (dedup pruning, visibility-timeout requeue, DLQ drain).
// It delegates to InMemoryBackend.pruneState so the handler and internal janitor share one code path.
func (j *Janitor) sweepExpiredMessages(ctx context.Context) {
	before := j.Backend.totalMessages()
	j.Backend.pruneState(time.Now())
	after := j.Backend.totalMessages()

	if purged := before - after; purged > 0 {
		telemetry.RecordWorkerItems(sqsJanitorService, sqsJanitorComponent, purged)
		logger.Load(ctx).InfoContext(ctx, "SQS janitor: expired messages purged", "count", purged)
	}
	telemetry.RecordWorkerTask(sqsJanitorService, sqsJanitorComponent, "success")
}
