package integration_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	kvssdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	kvstypes "github.com/aws/aws-sdk-go-v2/service/kinesisvideo/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createKinesisVideoClient(t *testing.T) *kvssdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err, "unable to load SDK config")

	return kvssdk.NewFromConfig(cfg, func(o *kvssdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_KinesisVideo_StreamLifecycle covers a stream's control-plane life via the
// real SDK client: create, ACTIVE, describe/list, retention/tags, data endpoint, delete.
func TestIntegration_KinesisVideo_StreamLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createKinesisVideoClient(t)
	ctx := t.Context()
	name := "it-stream-" + uuid.NewString()[:8]

	created, err := client.CreateStream(ctx, &kvssdk.CreateStreamInput{
		StreamName:           aws.String(name),
		DataRetentionInHours: aws.Int32(2),
		MediaType:            aws.String("video/h264"),
		Tags:                 map[string]string{"env": "it"},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStream(cleanupCtx, &kvssdk.DeleteStreamInput{StreamARN: created.StreamARN})
	})

	describe := func() *kvstypes.StreamInfo {
		out, descErr := client.DescribeStream(ctx, &kvssdk.DescribeStreamInput{StreamName: aws.String(name)})
		require.NoError(t, descErr)

		return out.StreamInfo
	}

	require.Eventually(t, func() bool {
		return describe().Status == kvstypes.StatusActive
	}, 15*time.Second, 100*time.Millisecond)

	info := describe()
	assert.Equal(t, aws.ToString(created.StreamARN), aws.ToString(info.StreamARN))
	assert.EqualValues(t, 2, aws.ToInt32(info.DataRetentionInHours))

	_, err = client.UpdateDataRetention(ctx, &kvssdk.UpdateDataRetentionInput{
		StreamName:                 aws.String(name),
		CurrentVersion:             info.Version,
		Operation:                  kvstypes.UpdateDataRetentionOperationIncreaseDataRetention,
		DataRetentionChangeInHours: aws.Int32(4),
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d := describe()

		return d.Status == kvstypes.StatusActive && aws.ToInt32(d.DataRetentionInHours) == 6
	}, 15*time.Second, 100*time.Millisecond)

	listed, err := client.ListStreams(ctx, &kvssdk.ListStreamsInput{
		StreamNameCondition: &kvstypes.StreamNameCondition{
			ComparisonOperator: kvstypes.ComparisonOperatorBeginsWith,
			ComparisonValue:    aws.String(name),
		},
	})
	require.NoError(t, err)
	assert.Len(t, listed.StreamInfoList, 1)

	_, err = client.TagStream(ctx, &kvssdk.TagStreamInput{
		StreamARN: created.StreamARN,
		Tags:      map[string]string{"team": "media"},
	})
	require.NoError(t, err)

	tags, err := client.ListTagsForStream(ctx, &kvssdk.ListTagsForStreamInput{StreamARN: created.StreamARN})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "it", "team": "media"}, tags.Tags)

	endpointOut, err := client.GetDataEndpoint(ctx, &kvssdk.GetDataEndpointInput{
		StreamName: aws.String(name),
		APIName:    kvstypes.APINamePutMedia,
	})
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(aws.ToString(endpointOut.DataEndpoint), "https://"))

	_, err = client.DeleteStream(ctx, &kvssdk.DeleteStreamInput{StreamARN: created.StreamARN})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, descErr := client.DescribeStream(ctx, &kvssdk.DescribeStreamInput{StreamName: aws.String(name)})

		return hasAPIErrorCode(descErr, "ResourceNotFoundException")
	}, 15*time.Second, 100*time.Millisecond)
}

// TestIntegration_KinesisVideo_SignalingChannelLifecycle covers a signaling
// channel from create through endpoint resolution to delete.
func TestIntegration_KinesisVideo_SignalingChannelLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createKinesisVideoClient(t)
	ctx := t.Context()
	name := "it-channel-" + uuid.NewString()[:8]

	created, err := client.CreateSignalingChannel(ctx, &kvssdk.CreateSignalingChannelInput{
		ChannelName: aws.String(name),
		ChannelType: kvstypes.ChannelTypeSingleMaster,
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteSignalingChannel(cleanupCtx, &kvssdk.DeleteSignalingChannelInput{
			ChannelARN: created.ChannelARN,
		})
	})

	describe := func() *kvstypes.ChannelInfo {
		out, descErr := client.DescribeSignalingChannel(ctx, &kvssdk.DescribeSignalingChannelInput{
			ChannelName: aws.String(name),
		})
		require.NoError(t, descErr)

		return out.ChannelInfo
	}

	require.Eventually(t, func() bool {
		return describe().ChannelStatus == kvstypes.StatusActive
	}, 15*time.Second, 100*time.Millisecond)

	info := describe()

	_, err = client.UpdateSignalingChannel(ctx, &kvssdk.UpdateSignalingChannelInput{
		ChannelARN:     created.ChannelARN,
		CurrentVersion: info.Version,
		SingleMasterConfiguration: &kvstypes.SingleMasterConfiguration{
			MessageTtlSeconds: aws.Int32(120),
		},
	})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		d := describe()

		return d.ChannelStatus == kvstypes.StatusActive &&
			aws.ToInt32(d.SingleMasterConfiguration.MessageTtlSeconds) == 120
	}, 15*time.Second, 100*time.Millisecond)

	endpoints, err := client.GetSignalingChannelEndpoint(ctx, &kvssdk.GetSignalingChannelEndpointInput{
		ChannelARN: created.ChannelARN,
		SingleMasterChannelEndpointConfiguration: &kvstypes.SingleMasterChannelEndpointConfiguration{
			Protocols: []kvstypes.ChannelProtocol{kvstypes.ChannelProtocolHttps, kvstypes.ChannelProtocolWss},
			Role:      kvstypes.ChannelRoleMaster,
		},
	})
	require.NoError(t, err)
	assert.Len(t, endpoints.ResourceEndpointList, 2)

	_, err = client.DeleteSignalingChannel(ctx, &kvssdk.DeleteSignalingChannelInput{ChannelARN: created.ChannelARN})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		_, descErr := client.DescribeSignalingChannel(ctx, &kvssdk.DescribeSignalingChannelInput{
			ChannelName: aws.String(name),
		})

		return hasAPIErrorCode(descErr, "ResourceNotFoundException")
	}, 15*time.Second, 100*time.Millisecond)
}
