package s3

import (
	"context"
	"log/slog"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultJanitorInterval = 500 * time.Millisecond

	// janitorBatchSize is the maximum number of objects deleted from a pending
	// bucket per janitor tick. This keeps each tick short while the queue is
	// visibly draining on the live metrics dashboard.
	janitorBatchSize = 100
)

// Janitor is the S3 background worker that drains buckets queued for async
// deletion and records queue-depth metrics for the live dashboard.
type Janitor struct {
	Backend  *InMemoryBackend
	Log      *slog.Logger
	Interval time.Duration
}

// NewJanitor creates a new S3 Janitor for the given backend.
func NewJanitor(backend *InMemoryBackend, log *slog.Logger) *Janitor {
	return &Janitor{
		Backend:  backend,
		Log:      log,
		Interval: defaultJanitorInterval,
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
			j.runOnce(ctx)
		}
	}
}

// runOnce performs one pass: records queue depth, then processes pending buckets.
func (j *Janitor) runOnce(ctx context.Context) {
	b := j.Backend

	// Snapshot pending bucket names under a short read-lock.
	b.mu.RLock("S3Janitor")
	pending := make([]string, 0)

	for name, bucket := range b.buckets {
		if bucket.DeletePending {
			pending = append(pending, name)
		}
	}
	b.mu.RUnlock()

	telemetry.RecordDeleteQueueDepth("s3", len(pending))

	for _, name := range pending {
		j.processBucket(ctx, name)
	}
}

// processBucket deletes up to janitorBatchSize objects from a pending bucket, then
// removes the bucket itself once it is empty.
func (j *Janitor) processBucket(ctx context.Context, name string) {
	b := j.Backend

	// Re-fetch the bucket (it may have been cleared by a concurrent run).
	b.mu.RLock("S3Janitor.processBucket")
	bucket, exists := b.buckets[name]
	b.mu.RUnlock()

	if !exists {
		return
	}

	// Delete a batch of objects under the bucket lock.
	bucket.mu.Lock("S3Janitor.processBucket")
	count := 0

	for key := range bucket.Objects {
		delete(bucket.Objects, key)
		count++

		if count >= janitorBatchSize {
			break
		}
	}

	remaining := len(bucket.Objects)
	bucket.mu.Unlock()

	if remaining > 0 {
		// More objects remain; they will be picked up on the next tick.
		return
	}

	// Bucket is empty — remove it from the global map.
	b.mu.Lock("S3Janitor.removeBucket")
	delete(b.buckets, name)
	b.mu.Unlock()

	j.Log.InfoContext(ctx, "S3 janitor: bucket deleted", "bucket", name)
}
