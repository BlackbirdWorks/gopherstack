package ec2

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultJanitorInterval   = time.Minute
	defaultTerminatedTTL     = time.Hour
	janitorWorkerComponent   = "TerminatedInstanceCleaner"
	janitorWorkerServiceName = "ec2"
)

// Janitor is the EC2 background worker that sweeps terminated instances after
// a configurable TTL, matching the AWS behavior where terminated instances
// remain visible for approximately one hour.
type Janitor struct {
	Backend       *InMemoryBackend
	Interval      time.Duration
	TerminatedTTL time.Duration
}

// NewJanitor creates a new EC2 Janitor for the given backend.
// If interval or terminatedTTL are zero, defaults are used.
func NewJanitor(backend *InMemoryBackend, interval, terminatedTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultJanitorInterval
	}

	if terminatedTTL == 0 {
		terminatedTTL = defaultTerminatedTTL
	}

	return &Janitor{
		Backend:       backend,
		Interval:      interval,
		TerminatedTTL: terminatedTTL,
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
			j.sweepTerminatedInstances(ctx)
		}
	}
}

// sweepTerminatedInstances removes instances that have been in the terminated
// state longer than TerminatedTTL and cleans up their tags.
func (j *Janitor) sweepTerminatedInstances(ctx context.Context) {
	cutoff := time.Now().Add(-j.TerminatedTTL)

	j.Backend.mu.Lock("EC2Janitor")

	var swept []string

	for id, inst := range j.Backend.instances {
		if inst.State == StateTerminated && !inst.TerminatedAt.IsZero() && inst.TerminatedAt.Before(cutoff) {
			swept = append(swept, id)
			delete(j.Backend.instances, id)
			delete(j.Backend.tags, id)
		}
	}

	j.Backend.mu.Unlock()

	count := len(swept)
	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(janitorWorkerServiceName, janitorWorkerComponent, count)
	telemetry.RecordWorkerTask(janitorWorkerServiceName, janitorWorkerComponent, "success")

	for _, id := range swept {
		logger.Load(ctx).InfoContext(ctx, "EC2 janitor: terminated instance swept", "instanceID", id)
	}
}
