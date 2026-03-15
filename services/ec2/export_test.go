package ec2

import (
	"context"
	"time"
)

// SweepTerminatedInstancesForTest exposes sweepTerminatedInstances for unit tests.
func (j *Janitor) SweepTerminatedInstancesForTest(ctx context.Context) {
	j.sweepTerminatedInstances(ctx)
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
