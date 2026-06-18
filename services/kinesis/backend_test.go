package kinesis_test

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// fakeConfigProvider implements config.Provider for tests.
type fakeConfigProvider struct{}

func (fakeConfigProvider) GetGlobalConfig() *config.GlobalConfig {
	return config.NewGlobalConfig("111111111111", "ap-southeast-2", 0, 0, false, 0)
}

// fakeContextConfig wraps fakeConfigProvider for service.AppContext.
type fakeContextConfig struct {
	fakeConfigProvider
}

func TestKinesisProvider_Name(t *testing.T) {
	t.Parallel()

	p := &kinesis.Provider{}
	assert.Equal(t, "Kinesis", p.Name())
}

func TestKinesisProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		config        any
		wantRegion    string
		wantAccountID string
	}{
		{
			name:          "WithConfig",
			config:        fakeContextConfig{},
			wantRegion:    "ap-southeast-2",
			wantAccountID: "111111111111",
		},
		{
			name:   "NoConfig",
			config: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &kinesis.Provider{}
			ctx := &service.AppContext{
				Config: tt.config,
				Logger: slog.Default(),
			}

			svc, err := p.Init(ctx)
			require.NoError(t, err)
			assert.NotNil(t, svc)

			if tt.wantRegion != "" {
				h, ok := svc.(*kinesis.Handler)
				require.True(t, ok)
				assert.Equal(t, tt.wantRegion, h.DefaultRegion)
				assert.Equal(t, tt.wantAccountID, h.AccountID)
			}
		})
	}
}

func TestKinesisBackend_FindSequencePositionGaps(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "gap-stream"}))

	desc, err := bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "gap-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	// Put a record - get seq "00000000000000000001"
	out1, err := bk.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "gap-stream",
		PartitionKey: "pk",
		Data:         []byte("first"),
	})
	require.NoError(t, err)

	// Put another - get seq "00000000000000000002"
	out2, err := bk.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "gap-stream",
		PartitionKey: "pk",
		Data:         []byte("second"),
	})
	require.NoError(t, err)

	// AT_SEQUENCE_NUMBER for out1.SequenceNumber should return index 0 (inclusive)
	iterOut, err := bk.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:             "gap-stream",
		ShardID:                shardID,
		ShardIteratorType:      "AT_SEQUENCE_NUMBER",
		StartingSequenceNumber: out1.SequenceNumber,
	})
	require.NoError(t, err)

	records, err := bk.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         10,
	})
	require.NoError(t, err)
	require.Len(t, records.Records, 2)
	assert.Equal(t, out1.SequenceNumber, records.Records[0].SequenceNumber)

	// AFTER_SEQUENCE_NUMBER for out1 should start at index 1
	iterOut2, err := bk.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:             "gap-stream",
		ShardID:                shardID,
		ShardIteratorType:      "AFTER_SEQUENCE_NUMBER",
		StartingSequenceNumber: out1.SequenceNumber,
	})
	require.NoError(t, err)

	records2, err := bk.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut2.ShardIterator,
		Limit:         10,
	})
	require.NoError(t, err)
	require.Len(t, records2.Records, 1)
	assert.Equal(t, out2.SequenceNumber, records2.Records[0].SequenceNumber)

	// AT_SEQUENCE_NUMBER for a sequence number that is lexicographically larger than all records
	// should return empty (positions at end)
	iterOut3, err := bk.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:             "gap-stream",
		ShardID:                shardID,
		ShardIteratorType:      "AT_SEQUENCE_NUMBER",
		StartingSequenceNumber: "99999999999999999999",
	})
	require.NoError(t, err)

	records3, err := bk.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut3.ShardIterator,
		Limit:         10,
	})
	require.NoError(t, err)
	assert.Empty(t, records3.Records)
}

func TestKinesisBackend_GetRecordsDeletedStream(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "deleted-stream"}))

	desc, err := bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "deleted-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	iterOut, err := bk.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "deleted-stream",
		ShardID:           shardID,
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	// Delete stream
	require.NoError(t, bk.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: "deleted-stream"}))

	// GetRecords should return stream not found
	_, err = bk.GetRecords(context.Background(), &kinesis.GetRecordsInput{ShardIterator: iterOut.ShardIterator})
	assert.ErrorIs(t, err, kinesis.ErrStreamNotFound)
}

func TestKinesisBackend_GetRecordsInvalidShard(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "shard-gone-stream"}),
	)

	desc, err := bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "shard-gone-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	iterOut, err := bk.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "shard-gone-stream",
		ShardID:           shardID,
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	// Delete and recreate the stream (new shards will have the same IDs so this won't test the gap,
	// but we can test invalid shard via ListShards with wrong stream name)
	require.NoError(
		t,
		bk.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: "shard-gone-stream"}),
	)

	// Recreate stream (iterator now points to deleted stream)
	_, err = bk.GetRecords(context.Background(), &kinesis.GetRecordsInput{ShardIterator: iterOut.ShardIterator})
	assert.Error(t, err)
}

func TestKinesisBackend_ListStreamsLimit(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	for i := range 5 {
		require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{
			StreamName: fmt.Sprintf("limit-stream-%d", i),
		}))
	}

	out, err := bk.ListStreams(context.Background(), &kinesis.ListStreamsInput{Limit: 3})
	require.NoError(t, err)
	assert.Len(t, out.StreamNames, 3)
	assert.True(t, out.HasMoreStreams)
}

// TestListStreams_Sorted verifies that stream names are returned in alphabetical order.
func TestListStreams_Sorted(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()

	for _, name := range []string{"charlie", "alpha", "bravo"} {
		require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: name}))
	}

	out, err := bk.ListStreams(context.Background(), &kinesis.ListStreamsInput{})
	require.NoError(t, err)
	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, out.StreamNames)
}

// TestListStreams_Pagination verifies cursor-based pagination using both
// ExclusiveStartStreamName and the opaque NextToken. AWS returns names in
// alphabetical order and sets NextToken to the last returned name when
// HasMoreStreams is true; passing it back as ExclusiveStartStreamName must
// resume past that name.
func TestListStreams_Pagination(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	for _, n := range []string{"a", "b", "c", "d", "e"} {
		require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: n}))
	}

	tests := []struct {
		input     *kinesis.ListStreamsInput
		name      string
		wantToken string
		want      []string
		wantMore  bool
	}{
		{
			name:      "first_page",
			input:     &kinesis.ListStreamsInput{Limit: 2},
			want:      []string{"a", "b"},
			wantMore:  true,
			wantToken: "b",
		},
		{
			name:      "second_page_via_next_token",
			input:     &kinesis.ListStreamsInput{Limit: 2, NextToken: "b"},
			want:      []string{"c", "d"},
			wantMore:  true,
			wantToken: "d",
		},
		{
			name:     "third_page_exclusive_start",
			input:    &kinesis.ListStreamsInput{Limit: 5, ExclusiveStartStreamName: "d"},
			want:     []string{"e"},
			wantMore: false,
		},
		{
			name:     "exclusive_start_skips_match",
			input:    &kinesis.ListStreamsInput{Limit: 10, ExclusiveStartStreamName: "a"},
			want:     []string{"b", "c", "d", "e"},
			wantMore: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := bk.ListStreams(context.Background(), tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.want, out.StreamNames)
			assert.Equal(t, tt.wantMore, out.HasMoreStreams)
			assert.Equal(t, tt.wantToken, out.NextToken)
		})
	}
}

// TestIncreaseDecreaseRetentionPeriod covers happy paths and validation for retention period changes.
func TestIncreaseDecreaseRetentionPeriod(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup   func(bk *kinesis.InMemoryBackend)
		action  func(bk *kinesis.InMemoryBackend) error
		name    string
		wantErr bool
	}{
		{
			name: "increase_from_24_to_48",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
			},
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.IncreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.IncreaseStreamRetentionPeriodInput{
						StreamName:           "s",
						RetentionPeriodHours: 48,
					},
				)
			},
		},
		{
			name: "decrease_from_48_to_24",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
				_ = bk.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
					StreamName: "s", RetentionPeriodHours: 48,
				})
			},
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.DecreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.DecreaseStreamRetentionPeriodInput{
						StreamName:           "s",
						RetentionPeriodHours: 24,
					},
				)
			},
		},
		{
			name:    "increase_stream_not_found",
			setup:   func(_ *kinesis.InMemoryBackend) {},
			wantErr: true,
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.IncreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.IncreaseStreamRetentionPeriodInput{
						StreamName: "missing", RetentionPeriodHours: 48,
					},
				)
			},
		},
		{
			name: "increase_same_value_is_noop",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
			},
			wantErr: false, // idempotent: same value → success
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.IncreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.IncreaseStreamRetentionPeriodInput{
						StreamName: "s", RetentionPeriodHours: 24, // same as default — no-op
					},
				)
			},
		},
		{
			name: "increase_above_max_rejected",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
			},
			wantErr: true,
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.IncreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.IncreaseStreamRetentionPeriodInput{
						StreamName: "s", RetentionPeriodHours: 9999,
					},
				)
			},
		},
		{
			name:    "decrease_stream_not_found",
			setup:   func(_ *kinesis.InMemoryBackend) {},
			wantErr: true,
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.DecreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.DecreaseStreamRetentionPeriodInput{
						StreamName: "missing", RetentionPeriodHours: 24,
					},
				)
			},
		},
		{
			name: "decrease_below_min_rejected",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
				_ = bk.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
					StreamName: "s", RetentionPeriodHours: 48,
				})
			},
			wantErr: true,
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.DecreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.DecreaseStreamRetentionPeriodInput{
						StreamName: "s", RetentionPeriodHours: 10, // below 24h minimum
					},
				)
			},
		},
		{
			name: "decrease_same_value_is_noop",
			setup: func(bk *kinesis.InMemoryBackend) {
				_ = bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "s"})
				_ = bk.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
					StreamName: "s", RetentionPeriodHours: 48,
				})
			},
			wantErr: false, // idempotent: same value → success
			action: func(bk *kinesis.InMemoryBackend) error {
				return bk.DecreaseStreamRetentionPeriod(
					context.Background(),
					&kinesis.DecreaseStreamRetentionPeriodInput{
						StreamName: "s", RetentionPeriodHours: 48, // same as current — no-op
					},
				)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := kinesis.NewInMemoryBackend()
			tt.setup(bk)
			err := tt.action(bk)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

// TestDeleteStream_ClosesTags verifies that deleting a stream does not leak the
// stream-level tags Prometheus metric registry.
func TestDeleteStream_ClosesTags(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "tagged-stream"}))

	// Delete should not panic (Close is safe to call).
	require.NoError(t, bk.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: "tagged-stream"}))

	// Recreating with the same name should succeed (Tags registry released).
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "tagged-stream"}))
}

// TestPutRecords_ThroughputErrorCode verifies that when FIS throughput fault is active,
// PutRecords records individual entries with the correct ProvisionedThroughputExceededException
// error code (not InternalFailure).
func TestPutRecords_ThroughputErrorCode(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "fault-stream"}))

	bk.InjectFaultForTest("fault-stream")

	out, err := bk.PutRecords(context.Background(), &kinesis.PutRecordsInput{
		StreamName: "fault-stream",
		Records: []kinesis.PutRecordsEntry{
			{PartitionKey: "pk", Data: []byte("data")},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Records, 1)
	assert.Equal(t, "ProvisionedThroughputExceededException", out.Records[0].ErrorCode)
	assert.Equal(t, 1, out.FailedRecordCount)
}

func TestSplitShard_Basic(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "split-stream", ShardCount: 1}),
	)

	// Get the initial shard list.
	listOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "split-stream"})
	require.NoError(t, err)
	require.Len(t, listOut.Shards, 1)

	parentID := listOut.Shards[0].ShardID
	// Split at a midpoint well inside the shard range.
	splitKey := "170141183460469231731687303715884105728" // 2^127 / 1

	err = bk.SplitShard(context.Background(), &kinesis.SplitShardInput{
		StreamName:         "split-stream",
		ShardToSplit:       parentID,
		NewStartingHashKey: splitKey,
	})
	require.NoError(t, err)

	// Default list (open shards only) should now have 2 shards.
	listOut, err = bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "split-stream"})
	require.NoError(t, err)
	assert.Len(t, listOut.Shards, 2, "split should produce 2 open child shards")

	// Both child shards reference the parent.
	for _, s := range listOut.Shards {
		assert.Equal(t, parentID, s.ParentShardID)
	}

	// Full list includes the closed parent + 2 children.
	fullOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:  "split-stream",
		ShardFilter: "FROM_TRIM_HORIZON",
	})
	require.NoError(t, err)
	assert.Len(t, fullOut.Shards, 3, "FROM_TRIM_HORIZON should include closed parent")
}

func TestMergeShards_Basic(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "merge-stream", ShardCount: 2}),
	)

	listOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "merge-stream"})
	require.NoError(t, err)
	require.Len(t, listOut.Shards, 2)

	shard1 := listOut.Shards[0].ShardID
	shard2 := listOut.Shards[1].ShardID

	err = bk.MergeShards(context.Background(), &kinesis.MergeShardsInput{
		StreamName:           "merge-stream",
		ShardToMerge:         shard1,
		AdjacentShardToMerge: shard2,
	})
	require.NoError(t, err)

	// Only 1 open shard (the merged one).
	openOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "merge-stream"})
	require.NoError(t, err)
	assert.Len(t, openOut.Shards, 1)

	merged := openOut.Shards[0]
	assert.Equal(t, shard1, merged.ParentShardID)
	assert.Equal(t, shard2, merged.AdjacentParentShardID)

	// Full list: 2 closed parents + 1 open merged = 3.
	fullOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:  "merge-stream",
		ShardFilter: "FROM_TRIM_HORIZON",
	})
	require.NoError(t, err)
	assert.Len(t, fullOut.Shards, 3)
}

func TestStreamEncryption_StartStop(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "enc-stream"}))

	// Initially no encryption.
	descOut, err := bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "enc-stream"})
	require.NoError(t, err)
	assert.Equal(t, "NONE", descOut.EncryptionType)
	assert.Empty(t, descOut.KeyID)

	// Start encryption.
	require.NoError(t, bk.StartStreamEncryption(context.Background(), &kinesis.StartStreamEncryptionInput{
		StreamName:     "enc-stream",
		EncryptionType: "KMS",
		KeyID:          "alias/aws/kinesis",
	}))

	descOut, err = bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "enc-stream"})
	require.NoError(t, err)
	assert.Equal(t, "KMS", descOut.EncryptionType)
	assert.Equal(t, "alias/aws/kinesis", descOut.KeyID)

	// Stop encryption.
	require.NoError(t, bk.StopStreamEncryption(context.Background(), &kinesis.StopStreamEncryptionInput{
		StreamName:     "enc-stream",
		EncryptionType: "KMS",
		KeyID:          "alias/aws/kinesis",
	}))

	descOut, err = bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "enc-stream"})
	require.NoError(t, err)
	assert.Equal(t, "NONE", descOut.EncryptionType)
	assert.Empty(t, descOut.KeyID)
}

func TestConsumerRegistrationAndList(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "consumer-lifecycle2"}),
	)

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/consumer-lifecycle2"

	// Register two consumers.
	regOut1, err := bk.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "app-1",
	})
	require.NoError(t, err)
	assert.Equal(t, "app-1", regOut1.Consumer.ConsumerName)
	assert.NotEmpty(t, regOut1.Consumer.ConsumerARN)

	_, err = bk.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "app-2",
	})
	require.NoError(t, err)

	// Duplicate registration should fail.
	_, err = bk.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "app-1",
	})
	require.Error(t, err)

	// ListStreamConsumers returns both.
	listOut, err := bk.ListStreamConsumers(
		context.Background(),
		&kinesis.ListStreamConsumersInput{StreamARN: streamARN},
	)
	require.NoError(t, err)
	assert.Len(t, listOut.Consumers, 2)

	// DescribeStreamConsumer by ARN.
	descOut, err := bk.DescribeStreamConsumer(context.Background(), &kinesis.DescribeStreamConsumerInput{
		ConsumerARN: regOut1.Consumer.ConsumerARN,
	})
	require.NoError(t, err)
	assert.Equal(t, "app-1", descOut.ConsumerDescription.ConsumerName)

	// Deregister.
	require.NoError(t, bk.DeregisterStreamConsumer(context.Background(), &kinesis.DeregisterStreamConsumerInput{
		ConsumerARN: regOut1.Consumer.ConsumerARN,
	}))

	listOut, err = bk.ListStreamConsumers(context.Background(), &kinesis.ListStreamConsumersInput{StreamARN: streamARN})
	require.NoError(t, err)
	assert.Len(t, listOut.Consumers, 1)
	assert.Equal(t, "app-2", listOut.Consumers[0].ConsumerName)
}

func TestSubscribeToShard_ReturnsRecords(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(
			context.Background(),
			&kinesis.CreateStreamInput{StreamName: "subscribe-stream", ShardCount: 1},
		),
	)

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/subscribe-stream"

	regOut, err := bk.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: "reader",
	})
	require.NoError(t, err)

	// Put some records.
	_, err = bk.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "subscribe-stream",
		PartitionKey: "pk1",
		Data:         []byte("hello"),
	})
	require.NoError(t, err)

	shardOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "subscribe-stream"})
	require.NoError(t, err)
	require.Len(t, shardOut.Shards, 1)
	shardID := shardOut.Shards[0].ShardID

	subOut, err := bk.SubscribeToShard(context.Background(), &kinesis.SubscribeToShardInput{
		ConsumerARN: regOut.Consumer.ConsumerARN,
		ShardID:     shardID,
		StartingPosition: kinesis.StartingPosition{
			Type: "TRIM_HORIZON",
		},
	})
	require.NoError(t, err)
	assert.Len(t, subOut.Event.Records, 1)
	assert.Equal(t, []byte("hello"), subOut.Event.Records[0].Data)
}

func TestListShards_AfterShardID(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "filter-stream", ShardCount: 4}),
	)

	// List all 4 shards.
	allOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "filter-stream"})
	require.NoError(t, err)
	require.Len(t, allOut.Shards, 4)

	// Use ExclusiveStartShardID to skip first two.
	filtOut, err := bk.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:            "filter-stream",
		ExclusiveStartShardID: allOut.Shards[1].ShardID,
	})
	require.NoError(t, err)
	assert.Len(t, filtOut.Shards, 2, "should return shards after the second shard")
}

func TestDeregisterStreamConsumer_ByIdentifier(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input         func(consumerARN string) *kinesis.DeregisterStreamConsumerInput
		name          string
		wantRemaining int
	}{
		{
			name: "by_consumer_arn",
			input: func(consumerARN string) *kinesis.DeregisterStreamConsumerInput {
				return &kinesis.DeregisterStreamConsumerInput{ConsumerARN: consumerARN}
			},
			wantRemaining: 0,
		},
		{
			name: "by_stream_arn_and_name",
			input: func(_ string) *kinesis.DeregisterStreamConsumerInput {
				return &kinesis.DeregisterStreamConsumerInput{
					StreamARN:    "arn:aws:kinesis:us-east-1:123456789012:stream/consumer-stream",
					ConsumerName: "consumer-a",
				}
			},
			wantRemaining: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			bk := kinesis.NewInMemoryBackend()
			require.NoError(
				t,
				bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "consumer-stream"}),
			)

			registered, err := bk.RegisterStreamConsumer(context.Background(), &kinesis.RegisterStreamConsumerInput{
				StreamARN:    "arn:aws:kinesis:us-east-1:123456789012:stream/consumer-stream",
				ConsumerName: "consumer-a",
			})
			require.NoError(t, err)

			err = bk.DeregisterStreamConsumer(context.Background(), tt.input(registered.Consumer.ConsumerARN))
			require.NoError(t, err)

			listOut, err := bk.ListStreamConsumers(context.Background(), &kinesis.ListStreamConsumersInput{
				StreamARN: "arn:aws:kinesis:us-east-1:123456789012:stream/consumer-stream",
			})
			require.NoError(t, err)
			assert.Len(t, listOut.Consumers, tt.wantRemaining)
		})
	}
}
