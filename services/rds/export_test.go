package rds

import (
	"context"
	"time"
)

// RDSIDFromARNForTest exposes rdsIDFromARN for unit tests.
func RDSIDFromARNForTest(arnOrID string) string {
	return rdsIDFromARN(arnOrID)
}

// InjectExpiredFaultForTest directly inserts an expired fisFailoverFault entry
// into the backend without starting a cleanup goroutine, allowing tests to
// exercise the lazy-eviction path in IsClusterFailoverActive.
func (b *InMemoryBackend) InjectExpiredFaultForTest(clusterID string) {
	b.mu.Lock("InjectExpiredFaultForTest")
	defer b.mu.Unlock()

	b.fisFailoverFaults[clusterID] = time.Now().Add(-time.Hour) // already expired
}

// ScheduleFailoverFaultCleanupForTest exposes scheduleFailoverFaultCleanup for tests.
func (b *InMemoryBackend) ScheduleFailoverFaultCleanupForTest(ctx context.Context, ids []string, dur time.Duration) {
	b.scheduleFailoverFaultCleanup(ctx, ids, dur)
}
