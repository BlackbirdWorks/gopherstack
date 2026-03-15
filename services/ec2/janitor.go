package ec2

import (
	"context"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/telemetry"
)

const (
	defaultJanitorInterval  = time.Minute
	defaultTerminatedTTL    = time.Hour
	defaultCancelledSpotTTL = 6 * time.Hour // AWS shows cancelled spot requests for ~6 hours

	instanceSweeperComponent = "TerminatedInstanceCleaner"
	spotSweeperComponent     = "CancelledSpotRequestCleaner"
	janitorWorkerServiceName = "ec2"
)

// Janitor is the EC2 background worker that sweeps terminated instances after
// a configurable TTL, matching the AWS behavior where terminated instances
// remain visible for approximately one hour. It also removes cancelled/closed
// spot instance requests after a separate TTL.
type Janitor struct {
	Backend          *InMemoryBackend
	Interval         time.Duration
	TerminatedTTL    time.Duration
	CancelledSpotTTL time.Duration
}

// NewJanitor creates a new EC2 Janitor for the given backend.
// If interval, terminatedTTL, or cancelledSpotTTL are zero, defaults are used.
func NewJanitor(backend *InMemoryBackend, interval, terminatedTTL, cancelledSpotTTL time.Duration) *Janitor {
	if interval == 0 {
		interval = defaultJanitorInterval
	}

	if terminatedTTL == 0 {
		terminatedTTL = defaultTerminatedTTL
	}

	if cancelledSpotTTL == 0 {
		cancelledSpotTTL = defaultCancelledSpotTTL
	}

	return &Janitor{
		Backend:          backend,
		Interval:         interval,
		TerminatedTTL:    terminatedTTL,
		CancelledSpotTTL: cancelledSpotTTL,
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
			j.sweepCancelledSpotRequests(ctx)
		}
	}
}

// sweepTerminatedInstances removes instances that have been in the terminated
// state longer than TerminatedTTL and cleans up their tags.
func (j *Janitor) sweepTerminatedInstances(ctx context.Context) {
	cutoff := time.Now().Add(-j.TerminatedTTL)

	j.Backend.mu.Lock("sweepTerminatedInstances")

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

	telemetry.RecordWorkerTask(janitorWorkerServiceName, instanceSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(janitorWorkerServiceName, instanceSweeperComponent, count)

	for _, id := range swept {
		logger.Load(ctx).InfoContext(ctx, "EC2 janitor: terminated instance swept", "instanceID", id)
	}
}

// sweepCancelledSpotRequests removes spot instance requests that have been in
// the "cancelled" or "closed" state longer than CancelledSpotTTL.
// In AWS, cancelled/closed spot requests remain visible for approximately
// 6 hours before they are permanently removed.
func (j *Janitor) sweepCancelledSpotRequests(ctx context.Context) {
	cutoff := time.Now().Add(-j.CancelledSpotTTL)

	j.Backend.mu.Lock("sweepCancelledSpotRequests")

	var swept []string

	for id, req := range j.Backend.spotRequests {
		terminal := req.State == "cancelled" || req.State == "closed"
		if terminal && !req.CancelledAt.IsZero() && req.CancelledAt.Before(cutoff) {
			swept = append(swept, id)
			delete(j.Backend.spotRequests, id)
			delete(j.Backend.tags, id)
		}
	}

	j.Backend.mu.Unlock()

	count := len(swept)

	telemetry.RecordWorkerTask(janitorWorkerServiceName, spotSweeperComponent, "success")

	if count == 0 {
		return
	}

	telemetry.RecordWorkerItems(janitorWorkerServiceName, spotSweeperComponent, count)

	for _, id := range swept {
		logger.Load(ctx).InfoContext(ctx, "EC2 janitor: cancelled spot request swept", "spotRequestID", id)
	}
}
