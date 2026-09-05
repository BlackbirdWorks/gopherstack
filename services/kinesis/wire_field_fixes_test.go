package kinesis_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

var errNoShards = errors.New("expected at least one shard back")

// TestStreamIdentifiedByARNOnly drives ops that AWS documents as accepting
// either StreamARN or StreamName ("When invoking this API, you must use
// either the StreamARN or the StreamName parameter, or both... It is
// recommended that you use the StreamARN input parameter", repeated on each
// op's own api_op_*.go) with StreamARN as the *only* stream identifier, no
// StreamName. Before this fix, gopherstack-enpq (cmd/structfielddiff) found
// AddTagsToStream/RemoveTagsFromStream/ListTagsForStream/
// IncreaseStreamRetentionPeriod/DecreaseStreamRetentionPeriod/ListShards/
// EnableEnhancedMonitoring/DisableEnhancedMonitoring/UpdateShardCount had no
// Go struct member for StreamARN at all -- an ARN-only caller (the
// AWS-recommended pattern) silently resolved to an empty stream name and
// failed with ResourceNotFoundException even though the ARN was valid.
func TestStreamIdentifiedByARNOnly(t *testing.T) {
	t.Parallel()

	cases := []struct {
		setup func(ctx context.Context, c *kinesissdk.Client, streamName string)
		call  func(ctx context.Context, c *kinesissdk.Client, streamARN string) error
		name  string
	}{
		{
			name: "add_tags_to_stream",
			call: func(ctx context.Context, c *kinesissdk.Client, arn string) error {
				_, err := c.AddTagsToStream(ctx, &kinesissdk.AddTagsToStreamInput{
					StreamARN: aws.String(arn),
					Tags:      map[string]string{"k": "v"},
				})

				return err
			},
		},
		{
			name: "remove_tags_from_stream",
			setup: func(ctx context.Context, c *kinesissdk.Client, streamName string) {
				_, _ = c.AddTagsToStream(ctx, &kinesissdk.AddTagsToStreamInput{
					StreamName: aws.String(streamName),
					Tags:       map[string]string{"k": "v"},
				})
			},
			call: func(ctx context.Context, c *kinesissdk.Client, arn string) error {
				_, err := c.RemoveTagsFromStream(ctx, &kinesissdk.RemoveTagsFromStreamInput{
					StreamARN: aws.String(arn),
					TagKeys:   []string{"k"},
				})

				return err
			},
		},
		{
			name: "list_tags_for_stream",
			call: func(ctx context.Context, c *kinesissdk.Client, arn string) error {
				_, err := c.ListTagsForStream(ctx, &kinesissdk.ListTagsForStreamInput{StreamARN: aws.String(arn)})

				return err
			},
		},
		{
			name: "increase_stream_retention_period",
			call: func(ctx context.Context, c *kinesissdk.Client, arn string) error {
				_, err := c.IncreaseStreamRetentionPeriod(ctx, &kinesissdk.IncreaseStreamRetentionPeriodInput{
					StreamARN:            aws.String(arn),
					RetentionPeriodHours: aws.Int32(48),
				})

				return err
			},
		},
		{
			name: "decrease_stream_retention_period",
			setup: func(ctx context.Context, c *kinesissdk.Client, streamName string) {
				_, _ = c.IncreaseStreamRetentionPeriod(ctx, &kinesissdk.IncreaseStreamRetentionPeriodInput{
					StreamName:           aws.String(streamName),
					RetentionPeriodHours: aws.Int32(48),
				})
			},
			call: func(ctx context.Context, c *kinesissdk.Client, arn string) error {
				_, err := c.DecreaseStreamRetentionPeriod(ctx, &kinesissdk.DecreaseStreamRetentionPeriodInput{
					StreamARN:            aws.String(arn),
					RetentionPeriodHours: aws.Int32(24),
				})

				return err
			},
		},
		{
			name: "list_shards",
			call: func(ctx context.Context, c *kinesissdk.Client, arn string) error {
				out, err := c.ListShards(ctx, &kinesissdk.ListShardsInput{StreamARN: aws.String(arn)})
				if err == nil && len(out.Shards) == 0 {
					err = errNoShards
				}

				return err
			},
		},
		{
			name: "enable_enhanced_monitoring",
			call: func(ctx context.Context, c *kinesissdk.Client, arn string) error {
				_, err := c.EnableEnhancedMonitoring(ctx, &kinesissdk.EnableEnhancedMonitoringInput{
					StreamARN:         aws.String(arn),
					ShardLevelMetrics: []types.MetricsName{types.MetricsNameIncomingBytes},
				})

				return err
			},
		},
		{
			name: "disable_enhanced_monitoring",
			setup: func(ctx context.Context, c *kinesissdk.Client, streamName string) {
				_, _ = c.EnableEnhancedMonitoring(ctx, &kinesissdk.EnableEnhancedMonitoringInput{
					StreamName:        aws.String(streamName),
					ShardLevelMetrics: []types.MetricsName{types.MetricsNameIncomingBytes},
				})
			},
			call: func(ctx context.Context, c *kinesissdk.Client, arn string) error {
				_, err := c.DisableEnhancedMonitoring(ctx, &kinesissdk.DisableEnhancedMonitoringInput{
					StreamARN:         aws.String(arn),
					ShardLevelMetrics: []types.MetricsName{types.MetricsNameIncomingBytes},
				})

				return err
			},
		},
		{
			name: "update_shard_count",
			call: func(ctx context.Context, c *kinesissdk.Client, arn string) error {
				_, err := c.UpdateShardCount(ctx, &kinesissdk.UpdateShardCountInput{
					StreamARN:        aws.String(arn),
					TargetShardCount: aws.Int32(2),
					ScalingType:      types.ScalingTypeUniformScaling,
				})

				return err
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := kinesis.NewInMemoryBackend()
			client := newTestKinesisClient(t, kinesis.NewHandler(backend))
			streamName := "arn-only-" + tc.name

			_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
				StreamName: aws.String(streamName),
				ShardCount: aws.Int32(1),
			})
			require.NoError(t, err)

			desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
				StreamName: aws.String(streamName),
			})
			require.NoError(t, err)
			streamARN := aws.ToString(desc.StreamDescription.StreamARN)

			if tc.setup != nil {
				tc.setup(t.Context(), client, streamName)
			}

			err = tc.call(t.Context(), client, streamARN)
			require.NoError(t, err, "op must succeed when the stream is identified by StreamARN alone, no StreamName")
		})
	}
}

// TestRegisterStreamConsumer_TagsRoundTrip drives RegisterStreamConsumer's
// real Tags parameter (kinesis@v1.46.4 api_op_RegisterStreamConsumer.go: "You
// can add tags to the registered consumer when making a RegisterStreamConsumer
// request by setting the Tags parameter") through the real SDK client and
// confirms ListTagsForResource against the *consumer's* ARN sees them --
// before this fix, RegisterStreamConsumerInput had no Go field for Tags at
// all (silently dropped), and ListTagsForResource/TagResource/UntagResource
// only ever resolved a stream ARN, never a consumer ARN.
func TestRegisterStreamConsumer_TagsRoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "consumer-tags-stream"

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)

	reg, err := client.RegisterStreamConsumer(t.Context(), &kinesissdk.RegisterStreamConsumerInput{
		StreamARN:    desc.StreamDescription.StreamARN,
		ConsumerName: aws.String("my-consumer"),
		Tags:         map[string]string{"team": "streaming"},
	})
	require.NoError(t, err)

	got, err := client.ListTagsForResource(t.Context(), &kinesissdk.ListTagsForResourceInput{
		ResourceARN: reg.Consumer.ConsumerARN,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "team", aws.ToString(got.Tags[0].Key))
	assert.Equal(t, "streaming", aws.ToString(got.Tags[0].Value))

	// TagResource/UntagResource against the consumer ARN must also work, not
	// just RegisterStreamConsumer's own Tags parameter.
	_, err = client.TagResource(t.Context(), &kinesissdk.TagResourceInput{
		ResourceARN: reg.Consumer.ConsumerARN,
		Tags:        map[string]string{"env": "prod"},
	})
	require.NoError(t, err)

	got, err = client.ListTagsForResource(t.Context(), &kinesissdk.ListTagsForResourceInput{
		ResourceARN: reg.Consumer.ConsumerARN,
	})
	require.NoError(t, err)
	assert.Len(t, got.Tags, 2)

	_, err = client.UntagResource(t.Context(), &kinesissdk.UntagResourceInput{
		ResourceARN: reg.Consumer.ConsumerARN,
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	got, err = client.ListTagsForResource(t.Context(), &kinesissdk.ListTagsForResourceInput{
		ResourceARN: reg.Consumer.ConsumerARN,
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
}

// TestDescribeStreamSummary_MaxRecordSizeAndWarmThroughput drives
// UpdateMaxRecordSize and UpdateStreamWarmThroughput, then confirms
// DescribeStreamSummary reports the new values back -- both are real,
// optional members of StreamDescriptionSummary
// (kinesis@v1.46.4 types/types.go), but gopherstack's DescribeStreamOutput
// carried neither, so a client had no way to read back settings it had
// itself just applied.
func TestDescribeStreamSummary_MaxRecordSizeAndWarmThroughput(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "summary-fields-stream"

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)

	before, err := client.DescribeStreamSummary(t.Context(), &kinesissdk.DescribeStreamSummaryInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1024), aws.ToInt32(before.StreamDescriptionSummary.MaxRecordSizeInKiB),
		"default 1 MiB record size must be reported even before any UpdateMaxRecordSize call")

	_, err = client.UpdateMaxRecordSize(t.Context(), &kinesissdk.UpdateMaxRecordSizeInput{
		StreamARN:          desc.StreamDescription.StreamARN,
		MaxRecordSizeInKiB: aws.Int32(2048),
	})
	require.NoError(t, err)

	_, err = client.UpdateStreamWarmThroughput(t.Context(), &kinesissdk.UpdateStreamWarmThroughputInput{
		StreamARN:           desc.StreamDescription.StreamARN,
		WarmThroughputMiBps: aws.Int32(5),
	})
	require.NoError(t, err)

	after, err := client.DescribeStreamSummary(t.Context(), &kinesissdk.DescribeStreamSummaryInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	assert.Equal(t, int32(2048), aws.ToInt32(after.StreamDescriptionSummary.MaxRecordSizeInKiB))
	require.NotNil(t, after.StreamDescriptionSummary.WarmThroughput)
	assert.Equal(t, int32(5), aws.ToInt32(after.StreamDescriptionSummary.WarmThroughput.CurrentMiBps))
	assert.Equal(t, int32(5), aws.ToInt32(after.StreamDescriptionSummary.WarmThroughput.TargetMiBps))
}

// TestCreateStream_MaxRecordSizeAndWarmThroughput drives CreateStreamInput's
// own MaxRecordSizeInKiB and WarmThroughputMiBps members (kinesis@v1.46.4
// api_op_CreateStream.go:101-121) -- distinct from the same-named fields on
// UpdateMaxRecordSizeInput/UpdateStreamWarmThroughputInput. Before this fix,
// CreateStream's decode struct had no members for either, so a caller
// specifying them at creation time got a stream silently pinned to the 1 MiB
// default record size and zero warm throughput.
func TestCreateStream_MaxRecordSizeAndWarmThroughput(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "create-stream-fields"

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName:          aws.String(streamName),
		ShardCount:          aws.Int32(1),
		MaxRecordSizeInKiB:  aws.Int32(2048),
		WarmThroughputMiBps: aws.Int32(5),
	})
	require.NoError(t, err)

	summary, err := client.DescribeStreamSummary(t.Context(), &kinesissdk.DescribeStreamSummaryInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)

	assert.Equal(t, int32(2048), aws.ToInt32(summary.StreamDescriptionSummary.MaxRecordSizeInKiB),
		"MaxRecordSizeInKiB given at CreateStream time must be applied")
	require.NotNil(t, summary.StreamDescriptionSummary.WarmThroughput)
	assert.Equal(t, int32(5), aws.ToInt32(summary.StreamDescriptionSummary.WarmThroughput.CurrentMiBps),
		"WarmThroughputMiBps given at CreateStream time must be applied")
}

// TestUpdateStreamMode_WarmThroughputMiBps drives UpdateStreamModeInput's
// WarmThroughputMiBps member (kinesis@v1.46.4 api_op_UpdateStreamMode.go,
// "valid when the stream mode is being updated to on-demand"). Before this
// fix, handleUpdateStreamMode's decode struct had no member for it at all,
// so it was silently dropped even on an ON_DEMAND transition.
func TestUpdateStreamMode_WarmThroughputMiBps(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "update-stream-mode-warm"

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)

	_, err = client.UpdateStreamMode(t.Context(), &kinesissdk.UpdateStreamModeInput{
		StreamARN: desc.StreamDescription.StreamARN,
		StreamModeDetails: &types.StreamModeDetails{
			StreamMode: types.StreamModeOnDemand,
		},
		WarmThroughputMiBps: aws.Int32(7),
	})
	require.NoError(t, err)

	summary, err := client.DescribeStreamSummary(t.Context(), &kinesissdk.DescribeStreamSummaryInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)

	require.NotNil(t, summary.StreamDescriptionSummary.WarmThroughput)
	assert.Equal(t, int32(7), aws.ToInt32(summary.StreamDescriptionSummary.WarmThroughput.CurrentMiBps),
		"WarmThroughputMiBps given at UpdateStreamMode time must be applied")
}

// TestGetRecords_EncryptionType drives types.Record's EncryptionType member
// (kinesis@v1.46.4 deserializers.go:5363, awsAwsjson11_deserializeDocumentRecord)
// on both GetRecords and SubscribeToShard. Before this fix, jsonRecord had no
// Go field for it at all -- every record silently reported no encryption type
// even on a stream with StartStreamEncryption(KMS) applied, though the
// backend already tracks Stream.EncryptionType and reads it back correctly
// on DescribeStream/DescribeStreamSummary.
func TestGetRecords_EncryptionType(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "encryption-type-stream"
	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.StreamDescription.Shards, errNoShards)
	shardID := desc.StreamDescription.Shards[0].ShardId

	_, err = client.StartStreamEncryption(t.Context(), &kinesissdk.StartStreamEncryptionInput{
		StreamName:     aws.String(streamName),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String("alias/test-key"),
	})
	require.NoError(t, err)

	_, err = client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("pk"),
		Data:         []byte("hello"),
	})
	require.NoError(t, err)

	iterOut, err := client.GetShardIterator(t.Context(), &kinesissdk.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           shardID,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	recOut, err := client.GetRecords(t.Context(), &kinesissdk.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	require.Len(t, recOut.Records, 1)
	assert.Equal(t, types.EncryptionTypeKms, recOut.Records[0].EncryptionType,
		"GetRecords must report the stream's real KMS encryption type, not the zero value")
}

// TestSubscribeToShard_EncryptionType is TestGetRecords_EncryptionType's
// enhanced-fan-out counterpart: SubscribeToShardEvent.Records use the same
// real types.Record shape (deserializers.go:5549-5605 ->
// awsAwsjson11_deserializeDocumentRecordList), so it shared the same missing
// jsonRecord.EncryptionType field.
func TestSubscribeToShard_EncryptionType(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamName := "subscribe-encryption-type-stream"
	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, desc.StreamDescription.Shards, errNoShards)
	shardID := desc.StreamDescription.Shards[0].ShardId

	_, err = client.StartStreamEncryption(t.Context(), &kinesissdk.StartStreamEncryptionInput{
		StreamName:     aws.String(streamName),
		EncryptionType: types.EncryptionTypeKms,
		KeyId:          aws.String("alias/test-key"),
	})
	require.NoError(t, err)

	consOut, err := client.RegisterStreamConsumer(t.Context(), &kinesissdk.RegisterStreamConsumerInput{
		StreamARN:    desc.StreamDescription.StreamARN,
		ConsumerName: aws.String("encryption-watcher"),
	})
	require.NoError(t, err)

	_, err = client.PutRecord(t.Context(), &kinesissdk.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("pk"),
		Data:         []byte("hello"),
	})
	require.NoError(t, err)

	out, err := client.SubscribeToShard(t.Context(), &kinesissdk.SubscribeToShardInput{
		ConsumerARN: consOut.Consumer.ConsumerARN,
		ShardId:     shardID,
		StartingPosition: &types.StartingPosition{
			Type: types.ShardIteratorTypeTrimHorizon,
		},
	})
	require.NoError(t, err)

	stream := out.GetStream()
	require.NotNil(t, stream)
	defer stream.Close()

	select {
	case ev := <-stream.Events():
		e, ok := ev.(*types.SubscribeToShardEventStreamMemberSubscribeToShardEvent)
		require.True(t, ok, "unexpected event type %T", ev)
		require.Len(t, e.Value.Records, 1)
		assert.Equal(t, types.EncryptionTypeKms, e.Value.Records[0].EncryptionType,
			"SubscribeToShard must report the stream's real KMS encryption type, not the zero value")
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an event from the real SDK's event stream reader")
	}
}
