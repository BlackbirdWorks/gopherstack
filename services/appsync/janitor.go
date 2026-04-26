package appsync

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
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
	ticker := time.NewTicker(j.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			evicted := j.Backend.SweepExpiredAPIKeys()
			telemetry.RecordWorkerItems("appsync", "APIKeySweeper", evicted)
			telemetry.RecordWorkerTask("appsync", "APIKeySweeper", "success")
		}
	}
}
