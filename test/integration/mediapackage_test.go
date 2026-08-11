package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediapackagesdk "github.com/aws/aws-sdk-go-v2/service/mediapackage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMediaPackageClient returns a MediaPackage client pointed at the shared test container.
func createMediaPackageClient(t *testing.T) *mediapackagesdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return mediapackagesdk.NewFromConfig(cfg, func(o *mediapackagesdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MediaPackage_ChannelLifecycle drives create→describe→list→delete of a channel.
func TestIntegration_MediaPackage_ChannelLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		name      string
		channelID string
	}{
		{name: "full_lifecycle", channelID: "integ-channel"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaPackageClient(t)

			createOut, err := client.CreateChannel(ctx, &mediapackagesdk.CreateChannelInput{
				Id:          aws.String(tt.channelID),
				Description: aws.String("integration test channel"),
			})
			require.NoError(t, err, "CreateChannel should succeed")
			assert.Equal(t, tt.channelID, aws.ToString(createOut.Id))
			assert.NotEmpty(t, aws.ToString(createOut.Arn), "channel ARN must be returned")

			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteChannel(
					cleanupCtx,
					&mediapackagesdk.DeleteChannelInput{Id: aws.String(tt.channelID)},
				)
			})

			descOut, err := client.DescribeChannel(
				ctx,
				&mediapackagesdk.DescribeChannelInput{Id: aws.String(tt.channelID)},
			)
			require.NoError(t, err, "DescribeChannel should succeed")
			assert.Equal(t, tt.channelID, aws.ToString(descOut.Id))

			listOut, err := client.ListChannels(ctx, &mediapackagesdk.ListChannelsInput{})
			require.NoError(t, err, "ListChannels should succeed")

			found := false
			for _, ch := range listOut.Channels {
				if aws.ToString(ch.Id) == tt.channelID {
					found = true

					break
				}
			}

			assert.True(t, found, "created channel should appear in list")

			_, err = client.DeleteChannel(ctx, &mediapackagesdk.DeleteChannelInput{Id: aws.String(tt.channelID)})
			require.NoError(t, err, "DeleteChannel should succeed")
		})
	}
}
