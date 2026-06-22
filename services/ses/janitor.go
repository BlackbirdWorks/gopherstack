package ses

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultSESJanitorInterval = time.Minute
	sesJanitorServiceName     = "ses"
	sesEmailSweeperComp       = "EmailSweeper"
)

// Janitor is the SES background worker that evicts emails older than the
// configured TTL to prevent unbounded memory and persistence growth.
type Janitor struct {
	Backend     *InMemoryBackend
	Interval    time.Duration
	TaskTimeout time.Duration
}

// NewJanitor creates a new SES Janitor for the given backend.
// A zero interval falls back to defaultSESJanitorInterval.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultSESJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	worker.RunTicker(ctx, sesJanitorServiceName, sesEmailSweeperComp, j.Interval, j.TaskTimeout, j.sweepExpiredEmails)
}

// SweepOnce executes a single TTL sweep. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredEmails(ctx)
}

func (j *Janitor) sweepExpiredEmails(ctx context.Context) {
	cutoff := time.Now().Add(-j.Backend.ttl())
	count := j.Backend.sweepExpiredEmails(cutoff)

	telemetry.RecordWorkerItems(sesJanitorServiceName, sesEmailSweeperComp, count)
	telemetry.RecordWorkerTask(sesJanitorServiceName, sesEmailSweeperComp, "success")

	if count > 0 {
		logger.Load(ctx).InfoContext(ctx, "SES janitor: expired emails evicted", "count", count)
	}
}
