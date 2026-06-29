package kinesis

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
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
	cancel   context.CancelFunc
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
	ctx, cancel := context.WithCancel(ctx)
	j.cancel = cancel

	g := worker.NewGroup(ctx, janitorServiceName)
	g.Ticker(
		retentionSweeperComp,
		j.Interval,
		j.TaskTimeout,
		j.sweepRetention,
	)

	<-ctx.Done()
	g.Stop()
}

// Stop explicitly shuts down the janitor.
func (j *Janitor) Stop() {
	if j.cancel != nil {
		j.cancel()
	}
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

	j.Backend.mu.RLock("KinesisJanitor")
	var streamsToSweep []*Stream
	for _, regionStreams := range j.Backend.streams {
		for _, stream := range regionStreams {
			streamsToSweep = append(streamsToSweep, stream)
		}
	}
	j.Backend.mu.RUnlock()

	for _, stream := range streamsToSweep {
		stream.mu.Lock("KinesisJanitor.stream")
		if stream.Status == streamStatusDeleting {
			stream.mu.Unlock()

			continue
		}
		cutoff := now.Add(-time.Duration(stream.RetentionPeriod) * time.Hour)

		for _, shard := range stream.Shards {
			totalTrimmed += shard.Records.trimBefore(cutoff)
		}
		stream.mu.Unlock()
	}

	telemetry.RecordWorkerTask(janitorServiceName, retentionSweeperComp, "success")

	if totalTrimmed == 0 {
		return
	}

	telemetry.RecordWorkerItems(janitorServiceName, retentionSweeperComp, totalTrimmed)

	logger.Load(ctx).
		InfoContext(ctx, "Kinesis janitor: expired records evicted", "count", totalTrimmed)
}
