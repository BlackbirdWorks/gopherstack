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

// InjectFaultForTest inserts an active (non-expired) throughput fault for testing.
func (b *InMemoryBackend) InjectFaultForTest(streamName string) {
	b.mu.Lock("InjectFaultForTest")
	defer b.mu.Unlock()

	b.fisThroughputFaults[streamName] = &kinesisThrottleFault{
		probability: 1.0,
	}
}

// HasFaultForTest reports whether a fault entry exists for streamName.
func (b *InMemoryBackend) HasFaultForTest(streamName string) bool {
	b.mu.RLock("HasFaultForTest")
	defer b.mu.RUnlock()

	_, ok := b.fisThroughputFaults[streamName]

	return ok
}

// ShardRecordCountForTest returns the number of records in shard i of the named stream.
func (b *InMemoryBackend) ShardRecordCountForTest(streamName string, shardIdx int) int {
	b.mu.RLock("ShardRecordCountForTest")
	defer b.mu.RUnlock()

	stream, ok := b.streams[streamName]
	if !ok || shardIdx >= len(stream.Shards) {
		return -1
	}

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

	stream, ok := b.streams[streamName]
	if !ok {
		return ErrStreamNotFound
	}

	stream.RetentionPeriod = hours

	return nil
}

// PushOldRecordForTest pushes a record into shard shardIdx of the named stream
// and backdates its ApproximateArrivalTimestamp by age.
func (b *InMemoryBackend) PushOldRecordForTest(streamName string, shardIdx int, age time.Duration) error {
	b.mu.Lock("PushOldRecordForTest")
	defer b.mu.Unlock()

	stream, ok := b.streams[streamName]
	if !ok {
		return ErrStreamNotFound
	}

	if shardIdx >= len(stream.Shards) {
		return ErrInvalidArgument
	}

	shard := stream.Shards[shardIdx]
	shard.nextSeq++
	rec := &Record{
		PartitionKey:                "test",
		Data:                        []byte("test"),
		SequenceNumber:              fmt.Sprintf("%020d", shard.nextSeq),
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

// StreamCount returns the number of streams in the backend.
func (b *InMemoryBackend) StreamCount() int {
	b.mu.RLock("StreamCount")
	defer b.mu.RUnlock()

	return len(b.streams)
}

// ResourcePolicyCount returns the number of resource policies in the backend.
func (b *InMemoryBackend) ResourcePolicyCount() int {
	b.mu.RLock("ResourcePolicyCount")
	defer b.mu.RUnlock()

	return len(b.resourcePolicies)
}

// HandlerOpsLen returns the number of pre-built handler ops.
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}
