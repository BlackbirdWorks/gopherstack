package appsync

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const defaultAppSyncJanitorInterval = 1 * time.Hour

// Janitor is the background worker for AppSync that prunes expired API keys.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new AppSync janitor.
func NewJanitor(backend *InMemoryBackend) *Janitor {
	return &Janitor{
		Backend:  backend,
		Interval: defaultAppSyncJanitorInterval,
	}
}

// Run executes the janitor loop.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, "appsync")
	g.Ticker("APIKeySweeper", j.Interval, 0, j.sweep)

	<-ctx.Done()
	g.Stop()
}

// sweep prunes expired API keys and records telemetry for the pass.
func (j *Janitor) sweep(_ context.Context) {
	evicted := j.Backend.SweepExpiredAPIKeys()
	telemetry.RecordWorkerItems("appsync", "APIKeySweeper", evicted)
	telemetry.RecordWorkerTask("appsync", "APIKeySweeper", "success")
}
