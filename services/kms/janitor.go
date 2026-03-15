package kms

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultKMSJanitorInterval = time.Minute
	kmsJanitorServiceName     = "kms"
	kmsJanitorComponent       = "KeyDeletionSweeper"
)

// Janitor is the KMS background worker that permanently deletes keys past their
// scheduled deletion date and purges the associated key material.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new KMS Janitor for the given backend.
// A zero interval falls back to defaultKMSJanitorInterval.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultKMSJanitorInterval
	}

	return &Janitor{
		Backend:  backend,
		Interval: interval,
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
			j.sweepExpiredKeys(ctx)
		}
	}
}

// SweepOnce executes a single deletion sweep. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredKeys(ctx)
}

// sweepExpiredKeys removes keys in PendingDeletion state whose deletion date has
// passed, permanently purging their key material and associated aliases and grants.
func (j *Janitor) sweepExpiredKeys(ctx context.Context) {
	now := float64(time.Now().UnixNano()) / nanoToSeconds
	purged := 0

	j.Backend.mu.Lock("KMSJanitor")

	for keyID, key := range j.Backend.keys {
		if key.KeyState != KeyStatePendingDeletion {
			continue
		}

		if key.DeletionDate == 0 || now < key.DeletionDate {
			continue
		}

		// Purge key material (current and history).
		delete(j.Backend.keyMaterials, keyID)
		delete(j.Backend.keyMaterialHistory, keyID)

		// Remove aliases pointing to this key.
		for aliasName, alias := range j.Backend.aliases {
			if alias.TargetKeyID == keyID {
				delete(j.Backend.aliases, aliasName)
			}
		}

		// Remove grants associated with this key.
		for grantID, grant := range j.Backend.grants {
			if grant.KeyID == keyID {
				delete(j.Backend.grants, grantID)
			}
		}

		// Remove the key itself.
		delete(j.Backend.keys, keyID)
		delete(j.Backend.policies, keyID)
		purged++
	}

	j.Backend.mu.Unlock()

	telemetry.RecordWorkerTask(kmsJanitorServiceName, kmsJanitorComponent, "success")

	if purged == 0 {
		return
	}

	telemetry.RecordWorkerItems(kmsJanitorServiceName, kmsJanitorComponent, purged)

	logger.Load(ctx).InfoContext(ctx, "KMS janitor: expired keys purged", "count", purged)
}
