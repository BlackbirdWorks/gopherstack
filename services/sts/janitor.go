package sts

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const defaultSTSJanitorInterval = 30 * time.Second

// Janitor is the STS background worker that evicts expired sessions to prevent
// unbounded growth of the sessions map under sustained load.
type Janitor struct {
	Backend     *InMemoryBackend
	Interval    time.Duration
	TaskTimeout time.Duration

	sweepCount       atomic.Int64
	expiredEvictions atomic.Int64
}

// JanitorMetrics provides janitor sweep counters.
type JanitorMetrics struct {
	SweepCount       int64
	ExpiredEvictions int64
}

// NewJanitor creates a new STS Janitor for the given backend.
// If interval is zero it falls back to defaultSTSJanitorInterval.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultSTSJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	worker.RunTicker(
		ctx,
		"sts",
		"SessionSweeper",
		j.Interval,
		j.TaskTimeout,
		j.sweepExpiredSessions,
	)
}

// SweepOnce runs a single sweep pass. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredSessions(ctx)
}

// sweepExpiredSessions removes sessions whose Expiration is in the past.
// The lock is held only long enough to copy expired keys, then released before deletion
// to reduce contention on hot paths.
func (j *Janitor) sweepExpiredSessions(ctx context.Context) {
	b := j.Backend

	b.mu.Lock()

	var expired []string

	for id, session := range b.sessions {
		if isSessionExpired(session) {
			expired = append(expired, id)
		}
	}

	for _, id := range expired {
		delete(b.sessions, id)
	}

	b.mu.Unlock()

	count := len(expired)
	// Count every sweep tick, even when no sessions are evicted.
	j.sweepCount.Add(1)
	j.expiredEvictions.Add(int64(count))

	telemetry.RecordWorkerItems("sts", "SessionSweeper", count)
	telemetry.RecordWorkerTask("sts", "SessionSweeper", "success")

	if count > 0 {
		logger.Load(ctx).InfoContext(ctx, "STS janitor: expired sessions evicted", "count", count)
	}
}

// Metrics returns cumulative janitor sweep counters.
func (j *Janitor) Metrics() JanitorMetrics {
	return JanitorMetrics{
		SweepCount:       j.sweepCount.Load(),
		ExpiredEvictions: j.expiredEvictions.Load(),
	}
}
