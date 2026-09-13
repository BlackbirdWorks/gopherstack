package kinesis_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// TestGetShardIterator_HonoursRetentionWindow covers gopherstack-s0ju item 3:
// GetShardIterator's TRIM_HORIZON and AT_TIMESTAMP must honor per-record
// ApproximateArrivalTimestamp against the stream's retention window
// (stream.RetentionPeriod, set via IncreaseStreamRetentionPeriod/
// DecreaseStreamRetentionPeriod), synchronously -- not only once the
// background janitor sweep (janitor.go) happens to have already evicted the
// expired record physically. See shard_iterators.go's use of
// retentionCutoff.
func TestGetShardIterator_HonoursRetentionWindow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		iterator      func() kinesis.GetShardIteratorInput
		name          string
		wantData      string
		retentionHrs  int
		expiredAgeHrs int
	}{
		{
			name:          "trim_horizon_excludes_expired_record",
			retentionHrs:  1,
			expiredAgeHrs: 2,
			iterator: func() kinesis.GetShardIteratorInput {
				return kinesis.GetShardIteratorInput{ShardIteratorType: "TRIM_HORIZON"}
			},
			wantData: "fresh",
		},
		{
			name:          "at_timestamp_older_than_retention_clamps_to_cutoff",
			retentionHrs:  1,
			expiredAgeHrs: 2,
			iterator: func() kinesis.GetShardIteratorInput {
				// A timestamp requested well before the retention cutoff
				// (24h ago, deep past both the 1h retention window and the
				// 2h-old expired record) must still resolve to only the
				// records within retention -- "If the time stamp is older
				// than the current trim horizon, the iterator returned is
				// for the oldest untrimmed data record (TRIM_HORIZON)."
				// (api_op_GetShardIterator.go).
				ts := time.Now().Add(-24 * time.Hour)

				return kinesis.GetShardIteratorInput{ShardIteratorType: "AT_TIMESTAMP", Timestamp: &ts}
			},
			wantData: "fresh",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kinesis.NewInMemoryBackend()
			ctx := context.Background()
			streamName := "retention-iter-" + tt.name

			require.NoError(t, b.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: streamName, ShardCount: 1}))
			require.NoError(t, b.SetRetentionPeriodForTest(streamName, tt.retentionHrs))
			require.NoError(t, b.PushOldRecordForTest(streamName, 0, time.Duration(tt.expiredAgeHrs)*time.Hour))

			_, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
				StreamName:   streamName,
				PartitionKey: "pk",
				Data:         []byte(tt.wantData),
			})
			require.NoError(t, err)

			input := tt.iterator()
			input.StreamName = streamName
			input.ShardID = "shardId-000000000000"

			itOut, err := b.GetShardIterator(ctx, &input)
			require.NoError(t, err)

			rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut.ShardIterator})
			require.NoError(t, err)
			require.Len(t, rOut.Records, 1, "only the record within the retention window should be returned")
			assert.Equal(t, tt.wantData, string(rOut.Records[0].Data))
		})
	}
}

// TestGetShardIterator_RetentionDecreaseAppliesBeforeJanitorSweep verifies
// that lowering a stream's retention (DecreaseStreamRetentionPeriod) takes
// effect for GetShardIterator's TRIM_HORIZON immediately -- before the
// background janitor sweep (janitor.go, real ticker interval) has had a
// chance to physically evict the now-expired record. This is the case the
// old "position 0 is always untrimmed" GetShardIterator logic could not
// honor: it trusted the ring buffer's physical contents, which lag a
// retention decrease by up to the janitor's polling interval.
func TestGetShardIterator_RetentionDecreaseAppliesBeforeJanitorSweep(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	ctx := context.Background()
	streamName := "retention-decrease-before-sweep"

	require.NoError(t, b.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: streamName, ShardCount: 1}))

	// Retention can only ever decrease to minRetentionHours (24h) at the
	// lowest, so widen it first: raise to 48h, then a 30h-old record sits
	// within that window but would fall outside the 24h floor once
	// decreased back down to it.
	require.NoError(t, b.IncreaseStreamRetentionPeriod(ctx, &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName: streamName, RetentionPeriodHours: 48,
	}))
	require.NoError(t, b.PushOldRecordForTest(streamName, 0, 30*time.Hour))
	_, err := b.PutRecord(
		ctx,
		&kinesis.PutRecordInput{StreamName: streamName, PartitionKey: "pk", Data: []byte("fresh")},
	)
	require.NoError(t, err)

	itBefore, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName: streamName, ShardID: "shardId-000000000000", ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)
	rBefore, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itBefore.ShardIterator})
	require.NoError(t, err)
	require.Len(t, rBefore.Records, 2, "sanity: both records are within the 48h retention")

	// Decrease retention to 24h: the 30h-old record is now outside the window.
	require.NoError(t, b.DecreaseStreamRetentionPeriod(ctx, &kinesis.DecreaseStreamRetentionPeriodInput{
		StreamName: streamName, RetentionPeriodHours: 24,
	}))

	// No janitor sweep has run -- the ring buffer still physically holds
	// both records. TRIM_HORIZON must still exclude the now-expired one.
	itAfter, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName: streamName, ShardID: "shardId-000000000000", ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)
	rAfter, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itAfter.ShardIterator})
	require.NoError(t, err)
	require.Len(t, rAfter.Records, 1,
		"TRIM_HORIZON must reflect the decreased retention synchronously, before any janitor sweep")
	assert.Equal(t, "fresh", string(rAfter.Records[0].Data))
}

// TestSubscribeToShard_HonoursRetentionWindow is
// TestGetShardIterator_HonoursRetentionWindow's SubscribeToShard
// counterpart: real AWS SubscribeToShard's StartingPosition TRIM_HORIZON/
// AT_TIMESTAMP semantics match GetShardIterator's, and this backend routes
// both through the same retentionCutoff (see consumers.go's
// subscribeToShardStartPos).
func TestSubscribeToShard_HonoursRetentionWindow(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	ctx := context.Background()
	streamName := "subscribe-retention"

	require.NoError(t, b.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: streamName, ShardCount: 1}))
	require.NoError(t, b.SetRetentionPeriodForTest(streamName, 1))
	require.NoError(t, b.PushOldRecordForTest(streamName, 0, 2*time.Hour))

	_, err := b.PutRecord(
		ctx,
		&kinesis.PutRecordInput{StreamName: streamName, PartitionKey: "pk", Data: []byte("fresh")},
	)
	require.NoError(t, err)

	regOut, err := b.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
		StreamARN:    "arn:aws:kinesis:us-east-1:123456789012:stream/" + streamName,
		ConsumerName: "retention-reader",
	})
	require.NoError(t, err)

	subOut, err := b.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
		ConsumerARN: regOut.Consumer.ConsumerARN,
		ShardID:     "shardId-000000000000",
		StartingPosition: kinesis.StartingPosition{
			Type: "TRIM_HORIZON",
		},
	})
	require.NoError(t, err)
	require.Len(t, subOut.Event.Records, 1, "only the record within the retention window should be delivered")
	assert.Equal(t, "fresh", string(subOut.Event.Records[0].Data))
}
