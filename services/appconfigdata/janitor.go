package appconfigdata

import (
	"context"
	"time"
)

const (
	defaultJanitorInterval = time.Hour
)

// Janitor is the AppConfig Data background worker that prunes expired retrieval sessions.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new AppConfig Data Janitor.
func NewJanitor(backend *InMemoryBackend) *Janitor {
	return &Janitor{
		Backend:  backend,
		Interval: defaultJanitorInterval,
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
			j.Backend.SweepExpiredSessions(ctx)
		}
	}
}
