package kinesisvideo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStreamLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{
		StreamName:           aws.String("life-stream"),
		DataRetentionInHours: aws.Int32(2),
	})
	require.NoError(t, err)

	desc := func() *types.StreamInfo {
		out, descErr := client.DescribeStream(ctx, &kinesisvideosdk.DescribeStreamInput{StreamARN: created.StreamARN})
		require.NoError(t, descErr)

		return out.StreamInfo
	}

	assert.Equal(t, types.StatusCreating, desc().Status)
	waitStreamActive(t, client, created.StreamARN)

	_, err = client.UpdateDataRetention(ctx, &kinesisvideosdk.UpdateDataRetentionInput{
		StreamARN:                  created.StreamARN,
		CurrentVersion:             desc().Version,
		Operation:                  types.UpdateDataRetentionOperationIncreaseDataRetention,
		DataRetentionChangeInHours: aws.Int32(1),
	})
	require.NoError(t, err)
	assert.Equal(t, types.StatusUpdating, desc().Status)
	waitStreamActive(t, client, created.StreamARN)

	_, err = client.DeleteStream(ctx, &kinesisvideosdk.DeleteStreamInput{StreamARN: created.StreamARN})
	require.NoError(t, err)

	_, err = client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{StreamName: aws.String("life-stream")})
	require.Error(t, err, "name stays reserved while DELETING")

	require.Eventually(t, func() bool {
		_, createErr := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{
			StreamName: aws.String("life-stream"),
		})

		return createErr == nil
	}, waitTimeout, waitTick)
}

func TestChannelLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateSignalingChannel(ctx, &kinesisvideosdk.CreateSignalingChannelInput{
		ChannelName: aws.String("life-channel"),
	})
	require.NoError(t, err)

	desc := func() *types.ChannelInfo {
		out, descErr := client.DescribeSignalingChannel(ctx, &kinesisvideosdk.DescribeSignalingChannelInput{
			ChannelARN: created.ChannelARN,
		})
		require.NoError(t, descErr)

		return out.ChannelInfo
	}

	assert.Equal(t, types.StatusCreating, desc().ChannelStatus)
	waitChannelActive(t, client, created.ChannelARN)

	_, err = client.UpdateSignalingChannel(ctx, &kinesisvideosdk.UpdateSignalingChannelInput{
		ChannelARN:     created.ChannelARN,
		CurrentVersion: desc().Version,
		SingleMasterConfiguration: &types.SingleMasterConfiguration{
			MessageTtlSeconds: aws.Int32(120),
		},
	})
	require.NoError(t, err)
	assert.Equal(t, types.StatusUpdating, desc().ChannelStatus)
	waitChannelActive(t, client, created.ChannelARN)
}
