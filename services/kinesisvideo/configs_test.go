package kinesisvideo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImageGenerationConfiguration(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateStream(
		ctx,
		&kinesisvideosdk.CreateStreamInput{StreamName: aws.String("image-cfg-stream")},
	)
	require.NoError(t, err)

	empty, err := client.DescribeImageGenerationConfiguration(
		ctx,
		&kinesisvideosdk.DescribeImageGenerationConfigurationInput{
			StreamARN: created.StreamARN,
		},
	)
	require.NoError(t, err)
	assert.Nil(t, empty.ImageGenerationConfiguration)

	_, err = client.UpdateImageGenerationConfiguration(ctx, &kinesisvideosdk.UpdateImageGenerationConfigurationInput{
		StreamARN: created.StreamARN,
		ImageGenerationConfiguration: &types.ImageGenerationConfiguration{
			DestinationConfig: &types.ImageGenerationDestinationConfig{
				DestinationRegion: aws.String("us-east-1"),
				Uri:               aws.String("s3://bucket/prefix"),
			},
			Format:            types.FormatJpeg,
			ImageSelectorType: types.ImageSelectorTypeServerTimestamp,
			SamplingInterval:  aws.Int32(1000),
			Status:            types.ConfigurationStatusEnabled,
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeImageGenerationConfiguration(
		ctx,
		&kinesisvideosdk.DescribeImageGenerationConfigurationInput{
			StreamARN: created.StreamARN,
		},
	)
	require.NoError(t, err)
	require.NotNil(t, out.ImageGenerationConfiguration)
	assert.Equal(t, types.FormatJpeg, out.ImageGenerationConfiguration.Format)
	assert.Equal(t, types.ConfigurationStatusEnabled, out.ImageGenerationConfiguration.Status)
	assert.EqualValues(t, 1000, aws.ToInt32(out.ImageGenerationConfiguration.SamplingInterval))
	require.NotNil(t, out.ImageGenerationConfiguration.DestinationConfig)
	assert.Equal(t, "s3://bucket/prefix", aws.ToString(out.ImageGenerationConfiguration.DestinationConfig.Uri))
}

func TestNotificationConfiguration(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateStream(
		ctx,
		&kinesisvideosdk.CreateStreamInput{StreamName: aws.String("notif-cfg-stream")},
	)
	require.NoError(t, err)

	empty, err := client.DescribeNotificationConfiguration(ctx, &kinesisvideosdk.DescribeNotificationConfigurationInput{
		StreamARN: created.StreamARN,
	})
	require.NoError(t, err)
	assert.Nil(t, empty.NotificationConfiguration)

	_, err = client.UpdateNotificationConfiguration(ctx, &kinesisvideosdk.UpdateNotificationConfigurationInput{
		StreamARN: created.StreamARN,
		NotificationConfiguration: &types.NotificationConfiguration{
			DestinationConfig: &types.NotificationDestinationConfig{Uri: aws.String("https://example.com/notify")},
			Status:            types.ConfigurationStatusEnabled,
		},
	})
	require.NoError(t, err)

	out, err := client.DescribeNotificationConfiguration(ctx, &kinesisvideosdk.DescribeNotificationConfigurationInput{
		StreamARN: created.StreamARN,
	})
	require.NoError(t, err)
	require.NotNil(t, out.NotificationConfiguration)
	assert.Equal(t, types.ConfigurationStatusEnabled, out.NotificationConfiguration.Status)
	require.NotNil(t, out.NotificationConfiguration.DestinationConfig)
	assert.Equal(t, "https://example.com/notify", aws.ToString(out.NotificationConfiguration.DestinationConfig.Uri))
}
