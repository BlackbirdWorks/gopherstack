package kinesis_test

import (
	"context"
	"errors"
	"testing"

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
