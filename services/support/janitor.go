package support

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

// RunJanitor periodically cleans up expired attachment sets and their associated attachments.
// It blocks until the context is cancelled.
func (b *InMemoryBackend) RunJanitor(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.sweepExpiredResources(ctx)
		}
	}
}

// sweepExpiredResources identifies and removes expired attachment sets and any orphaned attachments.
func (b *InMemoryBackend) sweepExpiredResources(ctx context.Context) {
	b.mu.Lock("sweepExpiredResources")
	defer b.mu.Unlock()

	now := time.Now()
	expiredSets := make([]string, 0, len(b.attachmentSets))

	for id, expiry := range b.attachmentSets {
		if now.After(expiry) {
			expiredSets = append(expiredSets, id)
		}
	}

	if len(expiredSets) == 0 {
		return
	}

	// Identify attachments to remove. We only remove attachments that are NOT linked
	// to any active communication. Since communications are linked to cases,
	// and cases are never deleted in this mock, we only prune attachments that
	// were part of an expired attachment set and never actually used in a communication.

	// Track which attachment sets are still in use by communications.
	usedSets := make(map[string]struct{})
	for _, comms := range b.communications {
		for _, c := range comms {
			if c.AttachmentSetID != "" {
				usedSets[c.AttachmentSetID] = struct{}{}
			}
		}
	}

	removedCount := 0
	for _, setID := range expiredSets {
		// Only delete if NOT used in a communication.
		if _, used := usedSets[setID]; !used {
			delete(b.attachmentSets, setID)
			removedCount++
		}
	}

	if removedCount > 0 {
		telemetry.RecordWorkerItems("support", "AttachmentCleaner", removedCount)
		logger.Load(ctx).DebugContext(ctx, "Support janitor: cleaned up expired attachment sets",
			"count", removedCount)
	}
	telemetry.RecordWorkerTask("support", "AttachmentCleaner", "success")
}
