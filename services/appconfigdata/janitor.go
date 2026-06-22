package appconfigdata

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	// DefaultJanitorInterval is how often the janitor sweeps expired sessions.
	DefaultJanitorInterval = time.Hour
)

// Janitor is the AppConfig Data background worker that prunes expired retrieval sessions.
type Janitor struct {
	Backend    *InMemoryBackend
	Interval   time.Duration
	SessionTTL time.Duration
}

// NewJanitor creates a new AppConfig Data Janitor with default settings.
func NewJanitor(backend *InMemoryBackend) *Janitor {
	return &Janitor{
		Backend:    backend,
		Interval:   DefaultJanitorInterval,
		SessionTTL: DefaultSessionTTL,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, "appconfigdata")
	g.Ticker("SessionSweeper", j.Interval, 0, j.sweep)

	<-ctx.Done()
	g.Stop()
}

// sweep prunes expired retrieval sessions.
func (j *Janitor) sweep(ctx context.Context) {
	j.Backend.SweepExpiredSessions(ctx, j.SessionTTL)
}
