package s3

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultJanitorInterval = 5 * time.Second

	// drainChunkSize is the maximum number of objects deleted per lock acquisition
	// while draining a DeletePending bucket. Between chunks the bucket lock is
	// released so that concurrent operations (PutObject, GetObject) are not
	// starved. 10 000 is fast enough (sub-millisecond in practice) while keeping
	// the critical section short.
	drainChunkSize = 10_000

	// maxConcurrentDrains is the maximum number of bucket drain goroutines that
	// may run simultaneously. Without a cap, thousands of pending-delete buckets
	// would each spawn a goroutine, all competing for the same locks and
	// exhausting goroutine and memory budgets.
	maxConcurrentDrains = 32

	// defaultDrainTimeout is applied per drain goroutine when TaskTimeout is
	// not configured. Five minutes is enough to drain even large buckets while
	// ensuring the goroutine doesn't leak past service shutdown.
	defaultDrainTimeout = 5 * time.Minute
)

// Janitor is the S3 background worker that drains buckets queued for async
// deletion and records queue-depth metrics for the live dashboard.
type Janitor struct {
	Backend      *InMemoryBackend
	drainSem     chan struct{}
	activeDrains sync.Map
	Interval     time.Duration
	TaskTimeout  time.Duration
}

// NewJanitor creates a new S3 Janitor for the given backend.
// The janitor interval is taken from the provided settings;
// if zero, it falls back to defaultJanitorInterval.
func NewJanitor(backend *InMemoryBackend, settings Settings) *Janitor {
	interval := settings.JanitorInterval
	if interval == 0 {
		interval = defaultJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
		drainSem: make(chan struct{}, maxConcurrentDrains),
	}
}

// Run runs the janitor loop until ctx is cancelled.
// Each tick, sweepAndDrain spawns one goroutine per pending bucket so that
// thousands of large buckets are drained in parallel rather than serially.
//
// The worker primitive recovers panics from each sweep automatically.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, "s3")
	g.Ticker("BucketCleaner", j.Interval, 0, j.sweep)

	<-ctx.Done()
	g.Stop()
}

// sweep performs one janitor tick. sweepAndDrain spawns long-lived drain
// goroutines that must outlive any per-tick task context, so it receives ctx
// directly; the lifecycle and multipart passes run under a TaskTimeout-bounded
// child context.
func (j *Janitor) sweep(ctx context.Context) {
	j.sweepAndDrain(ctx)

	taskCtx, cancel := j.taskContext(ctx)
	defer cancel()

	j.sweepLifecycle(taskCtx)
	j.cleanupDefaultMultipart(taskCtx)
}

// taskContext returns a child context bounded by TaskTimeout (if non-zero).
// The caller is responsible for calling the returned cancel function.
func (j *Janitor) taskContext(parent context.Context) (context.Context, context.CancelFunc) {
	if j.TaskTimeout > 0 {
		return context.WithTimeout(parent, j.TaskTimeout)
	}

	return context.WithCancel(parent)
}

// SweepOnce runs a single sweep pass (lifecycle + multipart cleanup). Exposed for testing.
// Note: sweepAndDrain is intentionally excluded here because it spawns long-lived
// drain goroutines that must outlive any per-task timeout context.
func (j *Janitor) SweepOnce(ctx context.Context) {
	taskCtx, cancel := j.taskContext(ctx)
	j.sweepLifecycle(taskCtx)
	j.cleanupDefaultMultipart(taskCtx)
	cancel()
}

// sweepAndDrain records queue depth and spawns a dedicated goroutine per
// pending bucket. A [sync.Map] (activeDrains) prevents duplicate goroutines for
// buckets that are still being drained from a previous tick.
func (j *Janitor) sweepAndDrain(ctx context.Context) {
	b := j.Backend

	b.mu.RLock("S3Janitor")
	all := b.buckets.All()
	pending := make([]string, 0, len(all))

	for _, bucket := range all {
		if bucket.DeletePending {
			pending = append(pending, bucket.Name)
		}
	}
	b.mu.RUnlock()

	telemetry.RecordWorkerQueueDepth("s3", "BucketCleaner", len(pending))
	telemetry.RecordWorkerTask("s3", "BucketCleaner", "success")

	for _, name := range pending {
		if _, loaded := j.activeDrains.LoadOrStore(name, struct{}{}); !loaded {
			// Acquire the semaphore slot before spawning. If the channel is full
			// (maxConcurrentDrains goroutines already running), skip this tick;
			// the bucket will be picked up on the next janitor tick.
			select {
			case j.drainSem <- struct{}{}:
			default:
				// All semaphore slots are taken; release the activeDrains entry
				// so the bucket can be picked up in a future tick.
				j.activeDrains.Delete(name)

				continue
			}

			go func(n string) {
				// recover() must be deferred FIRST (last to execute) so it catches panics
				// from processBucket. Cleanup (semaphore release) runs after recovery.
				defer func() {
					if r := recover(); r != nil {
						logger.Load(ctx).ErrorContext(ctx, "S3 janitor: panic in drain goroutine",
							"bucket", n, "panic", fmt.Sprintf("%v", r))
					}
				}()
				defer func() {
					<-j.drainSem
					j.activeDrains.Delete(n)
				}()

				drainCtx, cancel := j.newDrainContext(ctx)
				defer cancel()

				j.processBucket(drainCtx, n)
			}(name)
		}
	}
}

// newDrainContext creates a child context with the per-bucket drain timeout
// applied. If TaskTimeout is not configured, defaultDrainTimeout is used.
func (j *Janitor) newDrainContext(parent context.Context) (context.Context, context.CancelFunc) {
	timeout := j.TaskTimeout
	if timeout == 0 {
		timeout = defaultDrainTimeout
	}

	return context.WithTimeout(parent, timeout)
}

const defaultMultipartMaxAge = 24 * time.Hour

// cleanupDefaultMultipart aborts multipart uploads older than 24 hours.
//
// The 24h default applies UNCONDITIONALLY (including to buckets with lifecycle
// rules) because not every lifecycle configuration includes an
// AbortIncompleteMultipartUpload rule — without this floor, uploads can leak
// indefinitely. When a bucket's lifecycle DOES specify abort-incomplete with a
// shorter window, sweepLifecycle still runs first on the same tick and will
// remove uploads earlier; this pass is the safety net.
//
// Performance: expired upload IDs are collected under a read lock, then deleted
// under a write lock. This keeps the write-lock critical section proportional to
// the number of expired uploads rather than the total number of in-progress uploads.
func (j *Janitor) cleanupDefaultMultipart(_ context.Context) {
	b := j.Backend
	now := time.Now().UTC()
	abortBefore := now.Add(-defaultMultipartMaxAge)

	b.mu.RLock("S3Janitor.cleanupDefaultMultipart.scan")
	var expired []string

	for _, upload := range b.uploads.All() {
		if upload.Initiated.Before(abortBefore) {
			expired = append(expired, upload.UploadID)
		}
	}
	b.mu.RUnlock()

	if len(expired) == 0 {
		return
	}

	b.mu.Lock("S3Janitor.cleanupDefaultMultipart.delete")
	for _, uploadID := range expired {
		b.uploads.Delete(uploadID)
	}
	b.mu.Unlock()
}

// processBucket fully drains a pending bucket by deleting all objects in repeated
// chunks of drainChunkSize, releasing the bucket lock between chunks so that
// concurrent operations are not starved. Once the bucket is empty it removes all
// associated metadata (region index, uploads, tags) and closes the bucket mutex.
//
// Safe to call synchronously (directly for tests) or from a goroutine (production
// via sweepAndDrain). When called from a goroutine the activeDrains sentinel in
// sweepAndDrain prevents duplicate goroutines for the same bucket.
func (j *Janitor) processBucket(ctx context.Context, name string) {
	b := j.Backend

	// Locate the bucket once; buckets are keyed by name, so this is a single
	// O(1) lookup (bucket names cannot collide, let alone "move regions").
	b.mu.RLock("S3Janitor.processBucket")
	bucket, ok := b.buckets.Get(name)
	b.mu.RUnlock()

	if !ok {
		return
	}

	// Drain all objects in chunks, yielding between each to avoid starving
	// concurrent operations that are waiting on bucket.mu.
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		bucket.mu.Lock("S3Janitor.processBucket")
		count := deleteBatch(bucket.Objects, drainChunkSize)
		remaining := len(bucket.Objects)
		bucket.mu.Unlock()

		telemetry.RecordWorkerItems("s3", "BucketCleaner", count)

		if remaining > 0 {
			runtime.Gosched()

			continue
		}

		// Bucket is empty — remove it from the table and purge orphaned
		// uploads and tags to prevent resource leaks.
		b.mu.Lock("S3Janitor.removeBucket")
		b.buckets.Delete(name)
		b.purgeUploadsForBucketLocked(name)

		prefix := name + "/"
		for tagKey := range b.tags {
			if strings.HasPrefix(tagKey, prefix) {
				delete(b.tags, tagKey)
			}
		}

		b.mu.Unlock()
		bucket.mu.Close()

		logger.Load(ctx).InfoContext(ctx, "S3 janitor: bucket deleted", "bucket", name)

		return
	}
}

// abortStaleMultipartUploads removes multipart uploads for the given bucket that
// were initiated before abortBefore.
func (j *Janitor) abortStaleMultipartUploads(bucketName string, abortBefore time.Time) {
	b := j.Backend

	b.mu.Lock("S3Janitor.abortStaleMultipartUploads")
	defer b.mu.Unlock()

	// Copy matching upload IDs out of the index-owned group slice before
	// issuing any Delete, per [store.Index.Get]'s doc.
	grouped := b.uploadsByBucket.Get(bucketName)
	stale := make([]string, 0, len(grouped))

	for _, upload := range grouped {
		if upload.Initiated.Before(abortBefore) {
			stale = append(stale, upload.UploadID)
		}
	}

	for _, uploadID := range stale {
		b.uploads.Delete(uploadID)
	}
}
