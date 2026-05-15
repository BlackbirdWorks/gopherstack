package eventbridge

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
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
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.SweepOnce(ctx)
		}
	}
}

// SweepOnce executes one archive cleanup pass.
func (j *ArchiveJanitor) SweepOnce(ctx context.Context) {
	now := j.now()

	j.Backend.mu.Lock("EventBridgeArchiveJanitor")
	count := 0
	for name, archive := range j.Backend.archives {
		if archive.RetentionDays <= 0 {
			continue
		}

		expiry := archive.CreationTime.Add(time.Duration(archive.RetentionDays) * 24 * time.Hour)
		if now.Before(expiry) {
			continue
		}

		delete(j.Backend.archives, name)
		delete(j.Backend.archivedEvents, name)
		count++
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
