package azurequeue

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultJanitorInterval = time.Minute
	janitorService         = "azurequeue"
	janitorComponent       = "MessageExpirySweeper"
)

// Janitor is the Azure Queue background worker that deletes messages whose
// message TTL (see PutMessage's ttl parameter / DefaultMessageTTL) has
// elapsed, mirroring services/sqs's Janitor (SQS's MessageRetentionPeriod
// sweep).
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new Azure Queue Janitor for the given backend.
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
	g := worker.NewGroup(ctx, janitorService)
	g.Ticker(janitorComponent, j.Interval, 0, j.sweepExpiredMessages)

	<-ctx.Done()
	g.Stop()
}

// sweepExpiredMessages removes messages that have exceeded their TTL across
// every queue.
func (j *Janitor) sweepExpiredMessages(ctx context.Context) {
	purged := j.Backend.sweepExpired(j.Backend.now())

	if purged > 0 {
		telemetry.RecordWorkerItems(janitorService, janitorComponent, purged)
		logger.Load(ctx).InfoContext(ctx, "azurequeue janitor: expired messages purged", "count", purged)
	}

	telemetry.RecordWorkerTask(janitorService, janitorComponent, "success")
}
