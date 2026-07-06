package secretsmanager

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultSecretsManagerJanitorInterval = time.Minute
	secretsManagerJanitorService         = "secretsmanager"
	secretsManagerJanitorComponent       = "SecretDeletionSweeper"
	secondsPerDay                        = 24 * 3600
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
	g := worker.NewGroup(ctx, secretsManagerJanitorService)
	g.Ticker(
		secretsManagerJanitorComponent,
		j.Interval,
		j.TaskTimeout,
		j.sweepExpiredSecrets,
	)

	<-ctx.Done()
	g.Stop()
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

	for _, secret := range j.Backend.secrets.All() {
		if secret.DeletedDate == nil {
			continue
		}
		// Use ScheduledDeletionDate if set (reflects the actual RecoveryWindowInDays supplied at
		// delete time). Fall back to the default 30-day window for secrets deleted before this
		// field was introduced or force-deleted without a recovery window.
		var deletionTime float64
		if secret.ScheduledDeletionDate != nil {
			deletionTime = *secret.ScheduledDeletionDate
		} else {
			deletionTime = *secret.DeletedDate + float64(defaultRecoveryWindowDays*secondsPerDay)
		}
		if nowFloat >= deletionTime {
			if secret.Tags != nil {
				secret.Tags.Close()
			}
			j.Backend.secretDelete(secret.region, secret.Name)
			delete(j.Backend.resourcePoliciesStore(secret.region), secret.Name)
			delete(j.Backend.replicationConfigsStore(secret.region), secret.Name)
			purged++
		}
	}
	j.Backend.mu.Unlock()

	if purged > 0 {
		telemetry.RecordWorkerItems(
			secretsManagerJanitorService,
			secretsManagerJanitorComponent,
			purged,
		)
		logger.Load(ctx).
			InfoContext(ctx, "Secrets Manager janitor: expired secrets purged", "count", purged)
	}
	telemetry.RecordWorkerTask(
		secretsManagerJanitorService,
		secretsManagerJanitorComponent,
		"success",
	)
}
