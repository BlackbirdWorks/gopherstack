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

	for region, regionSecrets := range j.Backend.secrets {
		for name, secret := range regionSecrets {
			if secret.DeletedDate == nil {
				continue
			}
			// By default recovery window is 30 days. If the secret was deleted more than 30 days ago, purge it.
			deletionTime := *secret.DeletedDate + float64(defaultRecoveryWindowDays*secondsPerDay)
			if nowFloat >= deletionTime {
				if secret.Tags != nil {
					secret.Tags.Close()
				}
				delete(regionSecrets, name)
				delete(j.Backend.resourcePoliciesStore(region), name)
				delete(j.Backend.replicationConfigsStore(region), name)
				purged++
			}
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
