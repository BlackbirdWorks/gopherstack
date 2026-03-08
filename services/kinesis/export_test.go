package kinesis

import (
	"context"
	"time"
)

// ParseThrottlePercentageForTest exposes parseThrottlePercentage for unit tests.
func ParseThrottlePercentageForTest(s string) float64 {
	return parseThrottlePercentage(s)
}

// InjectExpiredThroughputFaultForTest directly inserts an expired fault entry
// for the given stream name without starting a cleanup goroutine, allowing
// tests to exercise the lazy-eviction path in isThroughputFaultActiveLocked.
func (b *InMemoryBackend) InjectExpiredThroughputFaultForTest(streamName string) {
	b.mu.Lock("InjectExpiredThroughputFaultForTest")
	defer b.mu.Unlock()

	b.fisThroughputFaults[streamName] = &kinesisThrottleFault{
		expiry:      time.Now().Add(-time.Hour), // already expired
		probability: 1.0,
	}
}

// ScheduleThroughputFaultCleanupForTest exposes scheduleThroughputFaultCleanup for tests.
func (b *InMemoryBackend) ScheduleThroughputFaultCleanupForTest(
	ctx context.Context,
	names []string,
	dur time.Duration,
) {
	b.scheduleThroughputFaultCleanup(ctx, names, dur)
}
