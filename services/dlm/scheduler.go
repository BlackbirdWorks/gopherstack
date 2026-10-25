package dlm

import (
	"context"
	"time"
)

const defaultSchedulerTick = 30 * time.Second

// Scheduler evaluates policy schedules on a ticker.
type Scheduler struct {
	backend *InMemoryBackend
	tick    time.Duration
}

// NewScheduler returns a scheduler for backend; a zero tick uses the default.
func NewScheduler(backend *InMemoryBackend, tick time.Duration) *Scheduler {
	if tick <= 0 {
		tick = defaultSchedulerTick
	}

	return &Scheduler{backend: backend, tick: tick}
}

// Run implements worker.Runner.
func (s *Scheduler) Run(ctx context.Context) {
	t := time.NewTicker(s.tick)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			s.backend.RunSchedules(ctx, now)
		}
	}
}
