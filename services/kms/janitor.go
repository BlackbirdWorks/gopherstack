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
	// TaskTimeout bounds each individual janitor task. When non-zero, each task
	// runs with a child context that expires after this duration, preventing a
	// stalled operation from blocking the janitor loop indefinitely.
	TaskTimeout time.Duration
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
			taskCtx, cancel := j.taskContext(ctx)
			j.sweepExpiredKeys(taskCtx)
			cancel()
		}
	}
}

// taskContext returns a child context bounded by TaskTimeout (if non-zero).
// The caller is responsible for calling the returned cancel function.
func (j *Janitor) taskContext(parent context.Context) (context.Context, context.CancelFunc) {
	if j.TaskTimeout > 0 {
		return context.WithTimeout(parent, j.TaskTimeout)
	}

	return context.WithCancel(parent)
}

// SweepOnce executes a single deletion sweep. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredKeys(ctx)
}

// sweepExpiredKeys removes keys in PendingDeletion state whose deletion date has
// passed, permanently purging their key material and associated aliases and grants.
// It also expires imported key material (EXTERNAL-origin keys) whose ValidTo has passed.
func (j *Janitor) sweepExpiredKeys(ctx context.Context) {
	now := float64(time.Now().UnixNano()) / nanoToSeconds
	purged := 0
	expired := 0

	j.Backend.mu.Lock("sweepExpiredKeys")

	for keyID, key := range j.Backend.keys {
		if key.KeyState == KeyStatePendingDeletion {
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

			continue
		}

		// Expire imported key material when the ValidTo date has passed.
		if key.Origin == KeyOriginExternal &&
			key.ExpirationModel == expirationModelExpires &&
			key.ValidTo > 0 &&
			now >= key.ValidTo &&
			key.KeyState == KeyStateEnabled {
			delete(j.Backend.keyMaterials, keyID)
			delete(j.Backend.keyMaterialHistory, keyID)
			key.KeyState = KeyStatePendingImport
			key.Enabled = false
			key.ValidTo = 0
			key.ExpirationModel = ""
			expired++
		}
	}

	if purged > 0 || expired > 0 {
		j.Backend.clearResolutionCache()
	}

	j.Backend.mu.Unlock()

	if purged > 0 {
		telemetry.RecordWorkerItems(kmsJanitorServiceName, kmsJanitorComponent, purged)
		logger.Load(ctx).InfoContext(ctx, "KMS janitor: expired keys purged", "count", purged)
	}

	if expired > 0 {
		telemetry.RecordWorkerItems(kmsJanitorServiceName, kmsJanitorComponent, expired)
		logger.Load(ctx).InfoContext(ctx, "KMS janitor: imported key material expired", "count", expired)
	}

	if purged == 0 && expired == 0 {
		telemetry.RecordWorkerTask(kmsJanitorServiceName, kmsJanitorComponent, "success")

		return
	}

	telemetry.RecordWorkerTask(kmsJanitorServiceName, kmsJanitorComponent, "success")
}
