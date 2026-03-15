package sts

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const defaultSTSJanitorInterval = 30 * time.Second

// Janitor is the STS background worker that evicts expired sessions to prevent
// unbounded growth of the sessions map under sustained load.
type Janitor struct {
	Backend  *InMemoryBackend
	Interval time.Duration
}

// NewJanitor creates a new STS Janitor for the given backend.
// If interval is zero it falls back to defaultSTSJanitorInterval.
func NewJanitor(backend *InMemoryBackend, interval time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultSTSJanitorInterval
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
			j.sweepExpiredSessions(ctx)
		}
	}
}

// sweepExpiredSessions removes sessions whose Expiration is in the past.
func (j *Janitor) sweepExpiredSessions(ctx context.Context) {
	b := j.Backend
	now := time.Now().UTC()

	b.mu.Lock()

	var expired []string

	for id, session := range b.sessions {
		if !session.Expiration.IsZero() && now.After(session.Expiration) {
			expired = append(expired, id)
		}
	}

	for _, id := range expired {
		delete(b.sessions, id)
	}

	b.mu.Unlock()

	count := len(expired)

	telemetry.RecordWorkerItems("sts", "SessionSweeper", count)
	telemetry.RecordWorkerTask("sts", "SessionSweeper", "success")

	if count > 0 {
		logger.Load(ctx).InfoContext(ctx, "STS janitor: expired sessions evicted", "count", count)
	}
}
