package kinesisvideo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateSignalingChannel(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	out, err := client.CreateSignalingChannel(ctx, &kinesisvideosdk.CreateSignalingChannelInput{
		ChannelName: aws.String("channel-one"),
		ChannelType: types.ChannelTypeSingleMaster,
		SingleMasterConfiguration: &types.SingleMasterConfiguration{
			MessageTtlSeconds: aws.Int32(120),
		},
	})
	require.NoError(t, err)
	assert.Contains(t, aws.ToString(out.ChannelARN), "channel/channel-one/")
}

func TestCreateSignalingChannel_DuplicateNameReturnsResourceInUse(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateSignalingChannel(ctx, &kinesisvideosdk.CreateSignalingChannelInput{
		ChannelName: aws.String("dup-channel"),
	})
	require.NoError(t, err)

	_, err = client.CreateSignalingChannel(ctx, &kinesisvideosdk.CreateSignalingChannelInput{
		ChannelName: aws.String("dup-channel"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceInUseException", apiErr.ErrorCode())
}

func TestDescribeSignalingChannel(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateSignalingChannel(ctx, &kinesisvideosdk.CreateSignalingChannelInput{
		ChannelName: aws.String("describe-channel"),
	})
	require.NoError(t, err)

	out, err := client.DescribeSignalingChannel(ctx, &kinesisvideosdk.DescribeSignalingChannelInput{
		ChannelARN: created.ChannelARN,
	})
	require.NoError(t, err)
	require.NotNil(t, out.ChannelInfo)
	assert.Equal(t, "describe-channel", aws.ToString(out.ChannelInfo.ChannelName))
	assert.Equal(t, types.ChannelTypeSingleMaster, out.ChannelInfo.ChannelType)
	assert.Equal(t, types.StatusActive, out.ChannelInfo.ChannelStatus)
	require.NotNil(t, out.ChannelInfo.SingleMasterConfiguration)
	assert.EqualValues(t, 60, aws.ToInt32(out.ChannelInfo.SingleMasterConfiguration.MessageTtlSeconds))
}

func TestDescribeSignalingChannel_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.DescribeSignalingChannel(t.Context(), &kinesisvideosdk.DescribeSignalingChannelInput{
		ChannelName: aws.String("missing"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}

func TestListSignalingChannels(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	for _, name := range []string{"cam-1", "cam-2", "door-1"} {
		_, err := client.CreateSignalingChannel(
			ctx,
			&kinesisvideosdk.CreateSignalingChannelInput{ChannelName: aws.String(name)},
		)
		require.NoError(t, err)
	}

	out, err := client.ListSignalingChannels(ctx, &kinesisvideosdk.ListSignalingChannelsInput{
		ChannelNameCondition: &types.ChannelNameCondition{
			ComparisonOperator: types.ComparisonOperatorBeginsWith,
			ComparisonValue:    aws.String("cam-"),
		},
	})
	require.NoError(t, err)
	require.Len(t, out.ChannelInfoList, 2)
}

func TestUpdateSignalingChannel(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateSignalingChannel(ctx, &kinesisvideosdk.CreateSignalingChannelInput{
		ChannelName: aws.String("update-channel"),
	})
	require.NoError(t, err)

	described, err := client.DescribeSignalingChannel(ctx, &kinesisvideosdk.DescribeSignalingChannelInput{
		ChannelARN: created.ChannelARN,
	})
	require.NoError(t, err)

	_, err = client.UpdateSignalingChannel(ctx, &kinesisvideosdk.UpdateSignalingChannelInput{
		ChannelARN:     created.ChannelARN,
		CurrentVersion: described.ChannelInfo.Version,
		SingleMasterConfiguration: &types.SingleMasterConfiguration{
			MessageTtlSeconds: aws.Int32(30),
		},
	})
	require.NoError(t, err)

	after, err := client.DescribeSignalingChannel(ctx, &kinesisvideosdk.DescribeSignalingChannelInput{
		ChannelARN: created.ChannelARN,
	})
	require.NoError(t, err)
	assert.EqualValues(t, 30, aws.ToInt32(after.ChannelInfo.SingleMasterConfiguration.MessageTtlSeconds))
	assert.NotEqual(t, aws.ToString(described.ChannelInfo.Version), aws.ToString(after.ChannelInfo.Version))
}

func TestUpdateSignalingChannel_VersionMismatch(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateSignalingChannel(ctx, &kinesisvideosdk.CreateSignalingChannelInput{
		ChannelName: aws.String("stale-channel"),
	})
	require.NoError(t, err)

	_, err = client.UpdateSignalingChannel(ctx, &kinesisvideosdk.UpdateSignalingChannelInput{
		ChannelARN:     created.ChannelARN,
		CurrentVersion: aws.String("not-the-real-version"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "VersionMismatchException", apiErr.ErrorCode())
}

func TestDeleteSignalingChannel(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateSignalingChannel(ctx, &kinesisvideosdk.CreateSignalingChannelInput{
		ChannelName: aws.String("delete-channel"),
	})
	require.NoError(t, err)

	_, err = client.DeleteSignalingChannel(ctx, &kinesisvideosdk.DeleteSignalingChannelInput{
		ChannelARN: created.ChannelARN,
	})
	require.NoError(t, err)

	_, err = client.DescribeSignalingChannel(ctx, &kinesisvideosdk.DescribeSignalingChannelInput{
		ChannelARN: created.ChannelARN,
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "ResourceNotFoundException", apiErr.ErrorCode())
}
