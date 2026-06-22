package timestreamwrite

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/worker"
)

const (
	defaultJanitorInterval = 5 * time.Minute
)

// Janitor is the Timestream background worker that enforces record retention.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new Timestream Janitor.
func NewJanitor(backend *InMemoryBackend) *Janitor {
	return &Janitor{
		Backend:  backend,
		Interval: defaultJanitorInterval,
	}
}

// Run runs the janitor loop until ctx is cancelled.
func (j *Janitor) Run(ctx context.Context) {
	g := worker.NewGroup(ctx, "timestreamwrite")
	g.Ticker("RetentionSweeper", j.Interval, 0, j.Backend.SweepRetention)

	<-ctx.Done()
	g.Stop()
}
