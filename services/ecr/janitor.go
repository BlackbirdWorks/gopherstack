package ecr

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultLifecycleJanitorInterval = time.Minute
	ecrWorkerService                = "ecr"
	lifecycleSweeperName            = "LifecycleExpiry"
)

// Janitor is the ECR background worker. It periodically evaluates every
// repository's lifecycle policy and deletes images that the policy selects for
// expiration, mirroring the AWS ECR lifecycle evaluation job — so count- and
// age-based expirations happen without any API call.
type Janitor struct {
	Backend     *InMemoryBackend
	Interval    time.Duration
	TaskTimeout time.Duration
}

// NewJanitor creates a Janitor for the given backend. A zero interval falls back
// to the default of one minute.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultLifecycleJanitorInterval
	}

	return &Janitor{Backend: backend, Interval: interval}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, ecrWorkerService)
	g.Ticker(lifecycleSweeperName, j.Interval, j.TaskTimeout, j.sweepLifecycle)

	<-ctx.Done()
	g.Stop()
}

// SweepOnce runs a single lifecycle-expiry sweep. Primarily intended for tests.
func (j *Janitor) SweepOnce(ctx context.Context) {
	j.sweepLifecycle(ctx)
}

// sweepLifecycle evaluates all lifecycle policies and deletes expired images.
func (j *Janitor) sweepLifecycle(ctx context.Context) {
	deleted := j.Backend.RunLifecycleExpiry(ctx)

	telemetry.RecordWorkerTask(ecrWorkerService, lifecycleSweeperName, "success")

	if deleted == 0 {
		return
	}

	telemetry.RecordWorkerItems(ecrWorkerService, lifecycleSweeperName, deleted)
	logger.Load(ctx).InfoContext(ctx,
		"ECR janitor: expired images per lifecycle policy", "deleted", deleted)
}
