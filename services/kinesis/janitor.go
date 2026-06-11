package kinesis

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultJanitorInterval = time.Minute
	janitorServiceName     = "kinesis"
	retentionSweeperComp   = "RetentionSweeper"
)

// Janitor is the Kinesis background worker that enforces per-stream retention
// periods by evicting records older than stream.RetentionPeriod hours.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
	// TaskTimeout bounds each individual janitor task. When non-zero, each task
	// runs with a child context that expires after this duration, preventing a
	// stalled operation from blocking the janitor loop indefinitely.
	TaskTimeout time.Duration
}

// NewJanitor creates a new Kinesis Janitor for the given backend.
// A zero interval falls back to defaultJanitorInterval.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultJanitorInterval
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
			taskCtx, cancel := j.taskContext(ctx)
			j.sweepRetention(taskCtx)
			cancel()
		}
	}
}

// taskContext returns a child context bounded by TaskTimeout (if non-zero).
// The caller is responsible for calling the returned cancel function.
func (j *Janitor) taskContext(parent context.Context) (context.Context, context.CancelFunc) {
	if j.TaskTimeout > 0 {
		return context.WithTimeout(parent, j.TaskTimeout)
	}

	return context.WithCancel(parent)
}

// SweepOnce executes a single retention sweep. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepRetention(ctx)
}

// sweepRetention removes records from every shard of every stream that are
// older than the stream's configured RetentionPeriod.
func (j *Janitor) sweepRetention(ctx context.Context) {
	now := time.Now()
	totalTrimmed := 0

	j.Backend.mu.Lock("KinesisJanitor")

	for _, regionStreams := range j.Backend.streams {
		for _, stream := range regionStreams {
			cutoff := now.Add(-time.Duration(stream.RetentionPeriod) * time.Hour)

			for _, shard := range stream.Shards {
				totalTrimmed += shard.Records.trimBefore(cutoff)
			}
		}
	}

	j.Backend.mu.Unlock()

	telemetry.RecordWorkerTask(janitorServiceName, retentionSweeperComp, "success")

	if totalTrimmed == 0 {
		return
	}

	telemetry.RecordWorkerItems(janitorServiceName, retentionSweeperComp, totalTrimmed)

	logger.Load(ctx).InfoContext(ctx, "Kinesis janitor: expired records evicted", "count", totalTrimmed)
}
