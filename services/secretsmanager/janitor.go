package secretsmanager

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultSecretsManagerJanitorInterval = time.Minute
	secretsManagerJanitorService         = "secretsmanager"
	secretsManagerJanitorComponent       = "SecretDeletionSweeper"
)

// Janitor is the Secrets Manager background worker that permanently deletes secrets
// past their recovery window.
type Janitor struct {
	Backend     *InMemoryBackend
	Interval    time.Duration
	TaskTimeout time.Duration
}

// NewJanitor creates a new Secrets Manager Janitor for the given backend.
// A zero interval falls back to defaultSecretsManagerJanitorInterval.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultSecretsManagerJanitorInterval
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
			j.sweepExpiredSecrets(taskCtx)
			cancel()
		}
	}
}

// taskContext returns a child context bounded by TaskTimeout (if non-zero).
func (j *Janitor) taskContext(parent context.Context) (context.Context, context.CancelFunc) {
	if j.TaskTimeout > 0 {
		return context.WithTimeout(parent, j.TaskTimeout)
	}

	return context.WithCancel(parent)
}

// SweepOnce executes a single deletion sweep. Exposed for testing.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepExpiredSecrets(ctx)
}

// sweepExpiredSecrets removes secrets whose soft-delete recovery window has passed.
func (j *Janitor) sweepExpiredSecrets(ctx context.Context) {
	nowFloat := float64(time.Now().UnixNano()) / nanoToSeconds

	j.Backend.mu.Lock("sweepExpiredSecrets")
	purged := 0

	for name, secret := range j.Backend.secrets {
		if secret.DeletedDate != nil {
			// By default recovery window is 30 days. If the secret was deleted more than 30 days ago, purge it.
			deletionTime := *secret.DeletedDate + float64(defaultRecoveryWindowDays*24*3600)
			if nowFloat >= deletionTime {
				if secret.Tags != nil {
					secret.Tags.Close()
				}
				delete(j.Backend.secrets, name)
				delete(j.Backend.resourcePolicies, name)
				delete(j.Backend.replicationConfigs, name)
				purged++
			}
		}
	}
	j.Backend.mu.Unlock()

	if purged > 0 {
		telemetry.RecordWorkerItems(secretsManagerJanitorService, secretsManagerJanitorComponent, purged)
		logger.Load(ctx).InfoContext(ctx, "Secrets Manager janitor: expired secrets purged", "count", purged)
	}
	telemetry.RecordWorkerTask(secretsManagerJanitorService, secretsManagerJanitorComponent, "success")
}
