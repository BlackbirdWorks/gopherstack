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

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	return h.janitor.TaskTimeout
}

// GetJanitorTerminatedTTL returns the TerminatedTTL configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the TTL.
func (h *Handler) GetJanitorTerminatedTTL() time.Duration {
	return h.janitor.TerminatedTTL
}

// GetJanitorCancelledSpotTTL returns the CancelledSpotTTL on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the TTL.
func (h *Handler) GetJanitorCancelledSpotTTL() time.Duration {
	return h.janitor.CancelledSpotTTL
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

// InjectOrphanedENIForTest injects a NetworkInterface directly into the backend
// map without going through TerminateInstances, simulating state restored from
// a snapshot that predates the ENI-cleanup fix. Used to test the janitor's
// defensive ENI sweep.
func (b *InMemoryBackend) InjectOrphanedENIForTest(eni *NetworkInterface) {
	b.mu.Lock("InjectOrphanedENIForTest")
	defer b.mu.Unlock()

	b.networkInterfaces[eni.ID] = eni
}
