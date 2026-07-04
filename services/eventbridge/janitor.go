package eventbridge

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const defaultArchiveJanitorInterval = time.Minute

// ArchiveJanitor removes expired archives based on RetentionDays.
type ArchiveJanitor struct {
	Backend  *InMemoryBackend
	now      func() time.Time
	Interval time.Duration
}

// NewArchiveJanitor creates an archive janitor for EventBridge.
func NewArchiveJanitor(backend *InMemoryBackend, interval time.Duration) *ArchiveJanitor {
	if interval <= 0 {
		interval = defaultArchiveJanitorInterval
	}

	return &ArchiveJanitor{
		Backend:  backend,
		Interval: interval,
		now:      time.Now,
	}
}

// Run executes the janitor loop until ctx is cancelled.
func (j *ArchiveJanitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, "eventbridge")
	g.Ticker("ArchiveJanitor", j.Interval, 0, j.SweepOnce)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce executes one archive cleanup pass.
func (j *ArchiveJanitor) SweepOnce(ctx context.Context) {
	now := j.now()

	j.Backend.mu.Lock("EventBridgeArchiveJanitor")
	count := 0
	for region, archives := range j.Backend.archives {
		for name, archive := range archives {
			if archive.RetentionDays <= 0 {
				continue
			}

			expiry := archive.CreationTime.Add(time.Duration(archive.RetentionDays) * 24 * time.Hour)
			if now.Before(expiry) {
				continue
			}

			delete(j.Backend.archives[region], name)
			delete(j.Backend.archivedEvents[region], name)
			count++
		}
	}
	j.Backend.mu.Unlock()

	j.Backend.patternCache.Clear()

	telemetry.RecordWorkerTask("eventbridge", "ArchiveJanitor", "success")
	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems("eventbridge", "ArchiveJanitor", count)
	logger.Load(ctx).InfoContext(ctx, "EventBridge archive janitor: expired archives removed", "count", count)
}
