package kinesis_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesissdk "github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesissdktypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

const channelTestServiceRoleARN = "arn:aws:iam::111122223333:role/kinesis-channel-role"

// createOnDemandStream creates an ON_DEMAND stream through the real SDK
// client and returns its ARN. CreateChannel is only supported for on-demand
// streams (api_op_CreateChannel.go doc comment).
func createOnDemandStream(t *testing.T, client *kinesissdk.Client, streamName string) string {
	t.Helper()

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String(streamName),
		StreamModeDetails: &kinesissdktypes.StreamModeDetails{
			StreamMode: kinesissdktypes.StreamModeOnDemand,
		},
	})
	require.NoError(t, err)

	desc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)

	return aws.ToString(desc.StreamDescription.StreamARN)
}

func minimalS3DestinationConfig() *kinesissdktypes.S3DestinationConfiguration {
	return &kinesissdktypes.S3DestinationConfiguration{
		StorageConfiguration: &kinesissdktypes.S3StorageConfiguration{
			BucketARN:           aws.String("arn:aws:s3:::my-channel-bucket"),
			CompressionType:     kinesissdktypes.S3CompressionTypeGzip,
			ExpectedBucketOwner: aws.String("111122223333"),
		},
	}
}

func minimalS3TablesDestinationConfig() *kinesissdktypes.S3TablesDestinationConfiguration {
	return &kinesissdktypes.S3TablesDestinationConfiguration{
		DeadLetterQueueS3Configuration: &kinesissdktypes.DeadLetterQueueS3Configuration{
			BucketARN:           aws.String("arn:aws:s3:::my-channel-dlq"),
			ExpectedBucketOwner: aws.String("111122223333"),
		},
		S3TablesConfigurationList: []kinesissdktypes.S3TablesConfiguration{
			{
				TableBucketARN:  aws.String("arn:aws:s3tables:us-east-1:111122223333:bucket/my-table-bucket"),
				Namespace:       aws.String("ns1"),
				TableName:       aws.String("tbl1"),
				CompressionType: kinesissdktypes.S3TablesCompressionTypeZstd,
			},
		},
	}
}

func minimalStreamConfigList(streamARN string) []kinesissdktypes.ChannelStreamConfiguration {
	return []kinesissdktypes.ChannelStreamConfiguration{
		{
			StreamARN: aws.String(streamARN),
			RecordConfiguration: &kinesissdktypes.RecordConfiguration{
				RecordFormatType: kinesissdktypes.RecordFormatTypeJson,
			},
		},
	}
}

// TestChannelLifecycle drives Create -> Describe -> List -> Update -> Delete
// through the real aws-sdk-go-v2 client, for both destination variants.
func TestChannelLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		buildS3       func() *kinesissdktypes.S3DestinationConfiguration
		buildS3Tables func() *kinesissdktypes.S3TablesDestinationConfiguration
		name          string
		id            string
		wantDestType  kinesissdktypes.ChannelDestinationType
	}{
		{
			name:         "s3",
			id:           "s3",
			buildS3:      minimalS3DestinationConfig,
			wantDestType: kinesissdktypes.ChannelDestinationTypeS3,
		},
		{
			name:          "s3 tables",
			id:            "s3-tables",
			buildS3Tables: minimalS3TablesDestinationConfig,
			wantDestType:  kinesissdktypes.ChannelDestinationTypeS3Tables,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := kinesis.NewInMemoryBackend()
			client := newTestKinesisClient(t, kinesis.NewHandler(backend))

			streamARN := createOnDemandStream(t, client, "channel-lifecycle-"+tc.id)

			createIn := &kinesissdk.CreateChannelInput{
				ChannelName:             aws.String("chan-" + tc.id),
				ServiceExecutionRoleARN: aws.String(channelTestServiceRoleARN),
				StreamConfigurationList: minimalStreamConfigList(streamARN),
				Tags:                    map[string]string{"env": "test"},
			}
			if tc.buildS3 != nil {
				createIn.S3DestinationConfiguration = tc.buildS3()
			}
			if tc.buildS3Tables != nil {
				createIn.S3TablesDestinationConfiguration = tc.buildS3Tables()
			}

			created, err := client.CreateChannel(t.Context(), createIn)
			require.NoError(t, err)
			require.NotNil(t, created.ChannelDescription)
			channelARN := aws.ToString(created.ChannelDescription.ChannelARN)
			assert.NotEmpty(t, channelARN)
			assert.Equal(t, kinesissdktypes.ChannelStatusActive, created.ChannelDescription.ChannelStatus)

			desc, err := client.DescribeChannel(t.Context(), &kinesissdk.DescribeChannelInput{
				ChannelARN: aws.String(channelARN),
			})
			require.NoError(t, err)
			assert.Equal(t, channelARN, aws.ToString(desc.ChannelDescription.ChannelARN))
			assert.Equal(t, kinesissdktypes.ChannelStatusActive, desc.ChannelDescription.ChannelStatus)

			listed, err := client.ListChannels(t.Context(), &kinesissdk.ListChannelsInput{
				StreamFilter: []kinesissdktypes.StreamFilter{{StreamARN: aws.String(streamARN)}},
			})
			require.NoError(t, err)
			require.Len(t, listed.ChannelSummaries, 1)
			assert.Equal(t, channelARN, aws.ToString(listed.ChannelSummaries[0].ChannelARN))
			assert.Equal(t, tc.wantDestType, listed.ChannelSummaries[0].ChannelDestinationType)

			updateIn := &kinesissdk.UpdateChannelInput{ChannelARN: aws.String(channelARN)}
			if tc.buildS3 != nil {
				updateIn.S3DestinationConfiguration = &kinesissdktypes.S3DestinationUpdateInput{
					DataFreshnessInSeconds: aws.Int32(600),
				}
			}
			if tc.buildS3Tables != nil {
				updateIn.S3TablesDestinationConfiguration = &kinesissdktypes.S3TablesDestinationUpdateInput{
					DataFreshnessInSeconds: aws.Int32(600),
				}
			}

			updated, err := client.UpdateChannel(t.Context(), updateIn)
			require.NoError(t, err)
			if tc.buildS3 != nil {
				require.NotNil(t, updated.ChannelDescription.S3DestinationConfiguration)
				assert.Equal(t, int32(600),
					aws.ToInt32(updated.ChannelDescription.S3DestinationConfiguration.DataFreshnessInSeconds))
			}
			if tc.buildS3Tables != nil {
				require.NotNil(t, updated.ChannelDescription.S3TablesDestinationConfiguration)
				assert.Equal(t, int32(600),
					aws.ToInt32(updated.ChannelDescription.S3TablesDestinationConfiguration.DataFreshnessInSeconds))
			}

			_, err = client.DeleteChannel(t.Context(), &kinesissdk.DeleteChannelInput{
				ChannelARN: aws.String(channelARN),
			})
			require.NoError(t, err)

			_, err = client.DescribeChannel(
				t.Context(),
				&kinesissdk.DescribeChannelInput{ChannelARN: aws.String(channelARN)},
			)
			require.Error(t, err)

			var rnf *kinesissdktypes.ResourceNotFoundException
			require.ErrorAs(t, err, &rnf)
		})
	}
}

// TestCreateChannel_Validation covers CreateChannel's documented rejection
// rules through the real SDK client.
func TestCreateChannel_Validation(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	onDemandARN := createOnDemandStream(t, client, "validation-on-demand")

	_, err := client.CreateStream(t.Context(), &kinesissdk.CreateStreamInput{
		StreamName: aws.String("validation-provisioned"),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)
	provisionedDesc, err := client.DescribeStream(t.Context(), &kinesissdk.DescribeStreamInput{
		StreamName: aws.String("validation-provisioned"),
	})
	require.NoError(t, err)
	provisionedARN := aws.ToString(provisionedDesc.StreamDescription.StreamARN)

	tests := []struct {
		mutate func(*kinesissdk.CreateChannelInput)
		name   string
		id     string
	}{
		{
			name: "neither destination",
			id:   "neither-destination",
			mutate: func(in *kinesissdk.CreateChannelInput) {
				in.StreamConfigurationList = minimalStreamConfigList(onDemandARN)
			},
		},
		{
			name: "both destinations",
			id:   "both-destinations",
			mutate: func(in *kinesissdk.CreateChannelInput) {
				in.StreamConfigurationList = minimalStreamConfigList(onDemandARN)
				in.S3DestinationConfiguration = minimalS3DestinationConfig()
				in.S3TablesDestinationConfiguration = minimalS3TablesDestinationConfig()
			},
		},
		{
			name: "provisioned stream rejected",
			id:   "provisioned-stream-rejected",
			mutate: func(in *kinesissdk.CreateChannelInput) {
				in.StreamConfigurationList = minimalStreamConfigList(provisionedARN)
				in.S3DestinationConfiguration = minimalS3DestinationConfig()
			},
		},
		{
			name: "unknown stream",
			id:   "unknown-stream",
			mutate: func(in *kinesissdk.CreateChannelInput) {
				in.StreamConfigurationList = minimalStreamConfigList(
					"arn:aws:kinesis:us-east-1:111122223333:stream/does-not-exist")
				in.S3DestinationConfiguration = minimalS3DestinationConfig()
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			in := &kinesissdk.CreateChannelInput{
				ChannelName:             aws.String("chan-" + tc.id),
				ServiceExecutionRoleARN: aws.String(channelTestServiceRoleARN),
			}
			tc.mutate(in)

			_, createErr := client.CreateChannel(t.Context(), in)
			require.Error(t, createErr)
		})
	}
}

// TestCreateChannel_DuplicateName confirms channel names are unique within
// account+region (CreateChannelInput.ChannelName doc comment).
func TestCreateChannel_DuplicateName(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamARN := createOnDemandStream(t, client, "duplicate-name-stream")

	in := &kinesissdk.CreateChannelInput{
		ChannelName:                aws.String("dup-channel"),
		ServiceExecutionRoleARN:    aws.String(channelTestServiceRoleARN),
		StreamConfigurationList:    minimalStreamConfigList(streamARN),
		S3DestinationConfiguration: minimalS3DestinationConfig(),
	}

	_, err := client.CreateChannel(t.Context(), in)
	require.NoError(t, err)

	_, err = client.CreateChannel(t.Context(), in)
	require.Error(t, err)

	var riu *kinesissdktypes.ResourceInUseException
	require.ErrorAs(t, err, &riu)
}

// TestChannelOps_NotFound covers Describe/Update/Delete against an unknown
// ChannelARN.
func TestChannelOps_NotFound(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	const bogusARN = "arn:aws:kinesis:us-east-1:111122223333:channel/does-not-exist"

	tests := []struct {
		call func() error
		name string
	}{
		{
			name: "describe",
			call: func() error {
				_, err := client.DescribeChannel(t.Context(), &kinesissdk.DescribeChannelInput{
					ChannelARN: aws.String(bogusARN),
				})

				return err
			},
		},
		{
			name: "update",
			call: func() error {
				_, err := client.UpdateChannel(t.Context(), &kinesissdk.UpdateChannelInput{
					ChannelARN: aws.String(bogusARN),
					S3DestinationConfiguration: &kinesissdktypes.S3DestinationUpdateInput{
						DataFreshnessInSeconds: aws.Int32(600),
					},
				})

				return err
			},
		},
		{
			name: "delete",
			call: func() error {
				_, err := client.DeleteChannel(t.Context(), &kinesissdk.DeleteChannelInput{
					ChannelARN: aws.String(bogusARN),
				})

				return err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := tc.call()
			require.Error(t, err)

			var rnf *kinesissdktypes.ResourceNotFoundException
			require.ErrorAs(t, err, &rnf)
		})
	}
}

// TestListChannels_Pagination proves NextToken/MaxResults paging over
// multiple channels registered on the same stream.
func TestListChannels_Pagination(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamARN := createOnDemandStream(t, client, "pagination-stream")

	const numChannels = 3
	names := make([]string, 0, numChannels)
	for i := range numChannels {
		name := "page-channel-" + string(rune('a'+i))
		names = append(names, name)

		_, err := client.CreateChannel(t.Context(), &kinesissdk.CreateChannelInput{
			ChannelName:                aws.String(name),
			ServiceExecutionRoleARN:    aws.String(channelTestServiceRoleARN),
			StreamConfigurationList:    minimalStreamConfigList(streamARN),
			S3DestinationConfiguration: minimalS3DestinationConfig(),
		})
		require.NoError(t, err)
	}

	page1, err := client.ListChannels(t.Context(), &kinesissdk.ListChannelsInput{MaxResults: aws.Int32(2)})
	require.NoError(t, err)
	require.Len(t, page1.ChannelSummaries, 2)
	require.NotNil(t, page1.NextToken)

	page2, err := client.ListChannels(t.Context(), &kinesissdk.ListChannelsInput{
		MaxResults: aws.Int32(2),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.ChannelSummaries, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))

	seen := map[string]bool{}
	for _, c := range append(page1.ChannelSummaries, page2.ChannelSummaries...) {
		seen[aws.ToString(c.ChannelName)] = true
	}
	for _, name := range names {
		assert.True(t, seen[name], "expected channel %q across pages", name)
	}
}

// TestChannelTags_RoundTrip proves CreateChannel's Tags and the generic
// ARN-based ListTagsForResource/TagResource/UntagResource ops all work
// against a channel ARN, mirroring the consumer-ARN routing precedent
// already established for RegisterStreamConsumer.
func TestChannelTags_RoundTrip(t *testing.T) {
	t.Parallel()

	backend := kinesis.NewInMemoryBackend()
	client := newTestKinesisClient(t, kinesis.NewHandler(backend))

	streamARN := createOnDemandStream(t, client, "tags-stream")

	created, err := client.CreateChannel(t.Context(), &kinesissdk.CreateChannelInput{
		ChannelName:                aws.String("tagged-channel"),
		ServiceExecutionRoleARN:    aws.String(channelTestServiceRoleARN),
		StreamConfigurationList:    minimalStreamConfigList(streamARN),
		S3DestinationConfiguration: minimalS3DestinationConfig(),
		Tags:                       map[string]string{"env": "test"},
	})
	require.NoError(t, err)
	channelARN := aws.ToString(created.ChannelDescription.ChannelARN)

	got, err := client.ListTagsForResource(t.Context(), &kinesissdk.ListTagsForResourceInput{
		ResourceARN: aws.String(channelARN),
	})
	require.NoError(t, err)
	require.Len(t, got.Tags, 1)
	assert.Equal(t, "env", aws.ToString(got.Tags[0].Key))
	assert.Equal(t, "test", aws.ToString(got.Tags[0].Value))

	_, err = client.TagResource(t.Context(), &kinesissdk.TagResourceInput{
		ResourceARN: aws.String(channelARN),
		Tags:        map[string]string{"team": "data"},
	})
	require.NoError(t, err)

	_, err = client.UntagResource(t.Context(), &kinesissdk.UntagResourceInput{
		ResourceARN: aws.String(channelARN),
		TagKeys:     []string{"env"},
	})
	require.NoError(t, err)

	final, err := client.ListTagsForResource(t.Context(), &kinesissdk.ListTagsForResourceInput{
		ResourceARN: aws.String(channelARN),
	})
	require.NoError(t, err)
	require.Len(t, final.Tags, 1)
	assert.Equal(t, "team", aws.ToString(final.Tags[0].Key))
}
