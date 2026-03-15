package ec2

import (
	"context"
	"time"
)

// SweepTerminatedInstancesForTest exposes sweepTerminatedInstances for unit tests.
func (j *Janitor) SweepTerminatedInstancesForTest(ctx context.Context) {
	j.sweepTerminatedInstances(ctx)
}

// SweepCancelledSpotRequestsForTest exposes sweepCancelledSpotRequests for unit tests.
func (j *Janitor) SweepCancelledSpotRequestsForTest(ctx context.Context) {
	j.sweepCancelledSpotRequests(ctx)
}

// SetInstanceTerminatedAtForTest sets the TerminatedAt field on an instance for testing.
// This allows tests to back-date the termination time to trigger immediate sweeping.
func (b *InMemoryBackend) SetInstanceTerminatedAtForTest(id string, t time.Time) {
	b.mu.Lock("SetInstanceTerminatedAtForTest")
	defer b.mu.Unlock()

	if inst, ok := b.instances[id]; ok {
		inst.TerminatedAt = t
	}
}

// SetSpotRequestCancelledAtForTest sets the CancelledAt field on a spot request for testing.
func (b *InMemoryBackend) SetSpotRequestCancelledAtForTest(id string, t time.Time) {
	b.mu.Lock("SetSpotRequestCancelledAtForTest")
	defer b.mu.Unlock()

	if req, ok := b.spotRequests[id]; ok {
		req.CancelledAt = t
	}
}
