package kinesis

import (
	"context"
	"fmt"
	"time"
)

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultJanitorInterval

// ParseThrottlePercentageForTest exposes parseThrottlePercentage for unit tests.
func ParseThrottlePercentageForTest(s string) float64 {
	return parseThrottlePercentage(s)
}

// InjectExpiredThroughputFaultForTest directly inserts an expired fault entry
// for the given stream name without starting a cleanup goroutine, allowing
// tests to exercise the lazy-eviction path in isThroughputFaultActive.
func (b *InMemoryBackend) InjectExpiredThroughputFaultForTest(streamName string) {
	b.faultsMu.Lock("InjectExpiredThroughputFaultForTest")
	defer b.faultsMu.Unlock()

	b.faultsStore(b.region)[streamName] = &kinesisThrottleFault{
		expiry:      time.Now().Add(-time.Hour), // already expired
		probability: 1.0,
	}
}

// ScheduleThroughputFaultCleanupForTest exposes scheduleThroughputFaultCleanup for tests.
// Names are resolved against the backend's default region.
func (b *InMemoryBackend) ScheduleThroughputFaultCleanupForTest(
	ctx context.Context,
	names []string,
	dur time.Duration,
) {
	targets := make([]regionStreamTarget, len(names))
	for i, n := range names {
		targets[i] = regionStreamTarget{region: b.region, name: n}
	}
	b.scheduleThroughputFaultCleanup(ctx, targets, dur)
}

// InjectFaultForTest inserts an active (non-expired) throughput fault for testing.
func (b *InMemoryBackend) InjectFaultForTest(streamName string) {
	b.faultsMu.Lock("InjectFaultForTest")
	defer b.faultsMu.Unlock()

	b.faultsStore(b.region)[streamName] = &kinesisThrottleFault{
		probability: 1.0,
	}
}

// HasFaultForTest reports whether a fault entry exists for streamName.
func (b *InMemoryBackend) HasFaultForTest(streamName string) bool {
	b.faultsMu.RLock("HasFaultForTest")
	defer b.faultsMu.RUnlock()

	_, ok := b.fisThroughputFaults[b.region][streamName]

	return ok
}

// ShardRecordCountForTest returns the number of records in shard i of the named stream.
func (b *InMemoryBackend) ShardRecordCountForTest(streamName string, shardIdx int) int {
	b.mu.RLock("ShardRecordCountForTest")

	stream, ok := b.streams.Get(streamKey(b.region, streamName))
	if !ok || shardIdx >= len(stream.Shards) {
		b.mu.RUnlock()

		return -1
	}
	stream.mu.RLock("ShardRecordCountForTest.stream")
	b.mu.RUnlock()
	defer stream.mu.RUnlock()

	return stream.Shards[shardIdx].Records.len()
}

// NewJanitorForTest creates a Janitor directly for whitebox tests.
func NewJanitorForTest(b *InMemoryBackend, interval time.Duration) *Janitor {
	return NewJanitor(b, interval)
}

// SweepOnceForTest exposes Janitor.SweepOnce for whitebox tests.
func (j *Janitor) SweepOnceForTest(ctx context.Context) {
	j.SweepOnce(ctx)
}

// SetRetentionPeriodForTest sets the retention period (in hours) of the named stream.
func (b *InMemoryBackend) SetRetentionPeriodForTest(streamName string, hours int) error {
	b.mu.Lock("SetRetentionPeriodForTest")
	defer b.mu.Unlock()

	stream, ok := b.streams.Get(streamKey(b.region, streamName))
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("SetRetentionPeriodForTest.stream")
	defer stream.mu.Unlock()

	stream.RetentionPeriod = hours

	return nil
}

// PushOldRecordForTest pushes a record into shard shardIdx of the named stream
// and backdates its ApproximateArrivalTimestamp by age.
func (b *InMemoryBackend) PushOldRecordForTest(streamName string, shardIdx int, age time.Duration) error {
	b.mu.Lock("PushOldRecordForTest")
	defer b.mu.Unlock()

	stream, ok := b.streams.Get(streamKey(b.region, streamName))
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("PushOldRecordForTest.stream")
	defer stream.mu.Unlock()

	if shardIdx >= len(stream.Shards) {
		return ErrInvalidArgument
	}

	shard := stream.Shards[shardIdx]
	shard.NextSeq++
	rec := &Record{
		PartitionKey:                "test",
		Data:                        []byte("test"),
		SequenceNumber:              fmt.Sprintf("%020d", shard.NextSeq),
		ApproximateArrivalTimestamp: time.Now().Add(-age),
	}
	shard.Records.push(rec)

	return nil
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.Interval
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// StreamCount returns the total number of streams in the backend across all regions.
func (b *InMemoryBackend) StreamCount() int {
	b.mu.RLock("StreamCount")
	defer b.mu.RUnlock()

	return b.streams.Len()
}

// ResourcePolicyCount returns the total number of resource policies in the backend
// across all regions.
func (b *InMemoryBackend) ResourcePolicyCount() int {
	b.mu.RLock("ResourcePolicyCount")
	defer b.mu.RUnlock()

	count := 0
	for _, regionPolicies := range b.resourcePolicies {
		count += len(regionPolicies)
	}

	return count
}

// HandlerOpsLen returns the number of pre-built handler ops.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}
