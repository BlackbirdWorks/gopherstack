package kinesisvideo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/aws/aws-sdk-go-v2/service/kinesisvideo/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUpdateImageGenerationConfiguration_JPEGQualityDefault(t *testing.T) {
	t.Parallel()

	cases := []struct {
		formatConfig map[string]string
		want         map[string]string
		format       types.Format
		name         string
	}{
		{name: "jpeg omitted gets 80", format: types.FormatJpeg, want: map[string]string{"JPEGQuality": "80"}},
		{
			name: "jpeg explicit kept", format: types.FormatJpeg,
			formatConfig: map[string]string{"JPEGQuality": "55"}, want: map[string]string{"JPEGQuality": "55"},
		},
		{name: "png untouched", format: types.FormatPng},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			client := newTestClient(t, newTestHandler())
			ctx := t.Context()

			created, err := client.CreateStream(ctx, &kinesisvideosdk.CreateStreamInput{StreamName: aws.String("jq")})
			require.NoError(t, err)

			_, err = client.UpdateImageGenerationConfiguration(
				ctx,
				&kinesisvideosdk.UpdateImageGenerationConfigurationInput{
					StreamARN: created.StreamARN,
					ImageGenerationConfiguration: &types.ImageGenerationConfiguration{
						DestinationConfig: &types.ImageGenerationDestinationConfig{
							DestinationRegion: aws.String("us-east-1"), Uri: aws.String("s3://bucket/p"),
						},
						Format:            tc.format,
						FormatConfig:      tc.formatConfig,
						ImageSelectorType: types.ImageSelectorTypeServerTimestamp,
						SamplingInterval:  aws.Int32(1000),
						Status:            types.ConfigurationStatusEnabled,
					},
				},
			)
			require.NoError(t, err)

			got, err := client.DescribeImageGenerationConfiguration(ctx,
				&kinesisvideosdk.DescribeImageGenerationConfigurationInput{StreamARN: created.StreamARN})
			require.NoError(t, err)
			require.NotNil(t, got.ImageGenerationConfiguration)
			assert.Equal(t, tc.want, got.ImageGenerationConfiguration.FormatConfig)
		})
	}
}
