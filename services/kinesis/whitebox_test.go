package kinesis

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setShardTimes directly sets shard shardIdx's StartedAt/ClosedAt for the
// named stream, letting ListShards' timestamp-bounded ShardFilter tests
// (AT_TIMESTAMP/FROM_TIMESTAMP/AT_TRIM_HORIZON) use deterministic timestamps
// instead of real wall-clock sleeps. A zero closedAt leaves the shard open.
func setShardTimes(b *InMemoryBackend, streamName string, shardIdx int, startedAt, closedAt time.Time) error {
	b.mu.Lock("test.setShardTimes")
	defer b.mu.Unlock()

	stream, ok := b.streams.Get(streamKey(b.region, streamName))
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("test.setShardTimes.stream")
	defer stream.mu.Unlock()

	if shardIdx >= len(stream.Shards) {
		return ErrInvalidArgument
	}

	shard := stream.Shards[shardIdx]
	shard.StartedAt = startedAt
	if !closedAt.IsZero() {
		shard.Closed = true
		shard.ClosedAt = closedAt
	}

	return nil
}

// TestListShards_ShardFilterType_AtTimestamp verifies AT_TIMESTAMP returns
// only shards that were open at the given instant: a shard closed before the
// query timestamp is excluded, a shard not yet started is excluded, and a
// shard open across the timestamp (or still open) is included.
func TestListShards_ShardFilterType_AtTimestamp(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	ctx := context.Background()
	require.NoError(t, b.CreateStream(ctx, &CreateStreamInput{
		StreamName: "at-ts-stream",
		ShardCount: 1,
	}))

	now := time.Now()
	oldStart := now.Add(-3 * time.Hour)
	oldClose := now.Add(-2 * time.Hour)

	// Reshard 1 -> 2: closes shard 0 and opens 2 new shards spanning the full
	// hash range (indices 1,2) -- UpdateShardCount's target is the absolute
	// new open shard count, not an increment.
	_, err := b.UpdateShardCount(ctx, &UpdateShardCountInput{
		StreamName:       "at-ts-stream",
		TargetShardCount: 2,
	})
	require.NoError(t, err)

	require.NoError(t, setShardTimes(b, "at-ts-stream", 0, oldStart, oldClose))
	newStart := now.Add(-2 * time.Hour)
	require.NoError(t, setShardTimes(b, "at-ts-stream", 1, newStart, time.Time{}))
	require.NoError(t, setShardTimes(b, "at-ts-stream", 2, newStart, time.Time{}))

	// While shard 0 was open (before it closed), the new shards didn't exist yet.
	duringOld := now.Add(-150 * time.Minute) // between oldStart and oldClose
	out, err := b.ListShards(ctx, &ListShardsInput{
		StreamName:           "at-ts-stream",
		ShardFilterType:      "AT_TIMESTAMP",
		ShardFilterTimestamp: &duringOld,
	})
	require.NoError(t, err)
	require.Len(t, out.Shards, 1)
	assert.Equal(t, "shardId-000000000000", out.Shards[0].ShardID)

	// After the reshard, only the new shards are open.
	afterReshard := now.Add(-90 * time.Minute)
	out2, err := b.ListShards(ctx, &ListShardsInput{
		StreamName:           "at-ts-stream",
		ShardFilterType:      "AT_TIMESTAMP",
		ShardFilterTimestamp: &afterReshard,
	})
	require.NoError(t, err)
	require.Len(t, out2.Shards, 2)
	assert.Equal(t, "shardId-000000000001", out2.Shards[0].ShardID)
	assert.Equal(t, "shardId-000000000002", out2.Shards[1].ShardID)
}

// TestListShards_ShardFilterType_FromTimestamp verifies FROM_TIMESTAMP
// returns every open shard plus closed shards whose end (ClosedAt) is at or
// after the query timestamp, excluding closed shards that ended earlier.
func TestListShards_ShardFilterType_FromTimestamp(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	ctx := context.Background()
	require.NoError(t, b.CreateStream(ctx, &CreateStreamInput{
		StreamName: "from-ts-stream",
		ShardCount: 1,
	}))

	now := time.Now()
	oldStart := now.Add(-3 * time.Hour)
	oldClose := now.Add(-2 * time.Hour)

	// Reshard 1 -> 2: closes shard 0 and opens 2 new shards (indices 1,2).
	_, err := b.UpdateShardCount(ctx, &UpdateShardCountInput{
		StreamName:       "from-ts-stream",
		TargetShardCount: 2,
	})
	require.NoError(t, err)

	require.NoError(t, setShardTimes(b, "from-ts-stream", 0, oldStart, oldClose))
	newStart := oldClose
	require.NoError(t, setShardTimes(b, "from-ts-stream", 1, newStart, time.Time{}))
	require.NoError(t, setShardTimes(b, "from-ts-stream", 2, newStart, time.Time{}))

	// Querying from before the old shard closed: it qualifies (ClosedAt >= ts),
	// plus the 2 always-open new shards -- all 3 shards.
	beforeClose := oldClose.Add(-30 * time.Minute)
	out, err := b.ListShards(ctx, &ListShardsInput{
		StreamName:           "from-ts-stream",
		ShardFilterType:      "FROM_TIMESTAMP",
		ShardFilterTimestamp: &beforeClose,
	})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 3)

	// Querying from well after the old shard closed: only the 2 new open shards.
	afterClose := now.Add(-30 * time.Minute)
	out2, err := b.ListShards(ctx, &ListShardsInput{
		StreamName:           "from-ts-stream",
		ShardFilterType:      "FROM_TIMESTAMP",
		ShardFilterTimestamp: &afterClose,
	})
	require.NoError(t, err)
	require.Len(t, out2.Shards, 2)
	assert.Equal(t, "shardId-000000000001", out2.Shards[0].ShardID)
	assert.Equal(t, "shardId-000000000002", out2.Shards[1].ShardID)
}

// TestListShards_ShardFilterType_AtTrimHorizon verifies AT_TRIM_HORIZON
// returns shards open at the retention window's start, and that a freshly
// created stream (younger than its own retention period) is clamped to
// return all its shards rather than incorrectly returning none.
func TestListShards_ShardFilterType_AtTrimHorizon(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	ctx := context.Background()
	require.NoError(t, b.CreateStream(ctx, &CreateStreamInput{
		StreamName: "trim-horizon-stream",
		ShardCount: 2,
	}))

	// Fresh stream, default 24h retention: trim horizon math alone would
	// predate the stream's creation, so it must clamp to "all shards".
	out, err := b.ListShards(ctx, &ListShardsInput{
		StreamName:      "trim-horizon-stream",
		ShardFilterType: "AT_TRIM_HORIZON",
	})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 2)

	// Shrink retention to 1h and backdate the shards so they fall outside
	// the (now non-clamped) retention window -- AT_TRIM_HORIZON must exclude them.
	require.NoError(t, b.SetRetentionPeriodForTest("trim-horizon-stream", 1))
	longAgo := time.Now().Add(-10 * time.Hour)
	closedLongAgo := time.Now().Add(-9 * time.Hour)
	require.NoError(t, setShardTimes(b, "trim-horizon-stream", 0, longAgo, closedLongAgo))
	require.NoError(t, setShardTimes(b, "trim-horizon-stream", 1, longAgo, closedLongAgo))

	out2, err := b.ListShards(ctx, &ListShardsInput{
		StreamName:      "trim-horizon-stream",
		ShardFilterType: "AT_TRIM_HORIZON",
	})
	require.NoError(t, err)
	assert.Empty(t, out2.Shards, "shards closed long before the retention window must be excluded")
}
