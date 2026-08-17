package support

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

// RunJanitor periodically cleans up expired attachment sets and their associated attachments.
// It blocks until the context is cancelled.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, interval time.Duration) {
	g := worker.NewGroup(ctx, "support")
	g.Ticker("AttachmentCleaner", interval, 0, b.sweepExpiredResources)

	<-ctx.Done()
	g.Stop()
}

// sweepExpiredResources identifies and removes expired attachment sets and any orphaned attachments.
func (b *InMemoryBackend) sweepExpiredResources(ctx context.Context) {
	b.mu.Lock("sweepExpiredResources")
	defer b.mu.Unlock()

	now := time.Now()
	expiredSets := make([]string, 0, b.attachmentSets.Len())

	b.attachmentSets.Range(func(set *AttachmentSet) bool {
		if now.After(set.Expiry) {
			expiredSets = append(expiredSets, set.ID)
		}

		return true
	})

	if len(expiredSets) == 0 {
		return
	}

	removedCount := 0
	for _, setID := range expiredSets {
		set, _ := b.attachmentSets.Get(setID)
		for _, attachmentID := range set.AttachmentIDs {
			b.attachments.Delete(attachmentID)
		}
		b.attachmentSets.Delete(setID)
		removedCount++
	}

	if removedCount > 0 {
		telemetry.RecordWorkerItems("support", "AttachmentCleaner", removedCount)
		logger.Load(ctx).DebugContext(ctx, "Support janitor: cleaned up expired attachment sets",
			"count", removedCount)
	}
	telemetry.RecordWorkerTask("support", "AttachmentCleaner", "success")
}
