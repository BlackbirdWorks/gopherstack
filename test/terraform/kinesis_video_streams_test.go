package terraform_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kinesisvideosdk "github.com/aws/aws-sdk-go-v2/service/kinesisvideo"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createKinesisVideoClient returns a Kinesis Video Streams client pointed at the shared test container.
func createKinesisVideoClient(t *testing.T) *kinesisvideosdk.Client {
	t.Helper()

	return createClientWithEndpoint(t, kinesisvideosdk.NewFromConfig, endpoint)
}

// TestTerraform_KinesisVideoStreams provisions an aws_kinesis_video_stream via
// Terraform, then verifies it is visible via the Kinesis Video Streams SDK
// with the expected data retention and tags.
func TestTerraform_KinesisVideoStreams(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "kinesis-video-streams",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{
					"StreamName": "tf-kvs-" + uuid.NewString()[:8],
				}
			},
			verify: func(t *testing.T, ctx context.Context, vars map[string]any) {
				t.Helper()

				client := createKinesisVideoClient(t)
				streamName := vars["StreamName"].(string)

				out, err := client.DescribeStream(ctx, &kinesisvideosdk.DescribeStreamInput{
					StreamName: aws.String(streamName),
				})
				require.NoError(t, err, "DescribeStream should succeed after terraform apply")
				require.NotNil(t, out.StreamInfo)
				assert.Equal(t, streamName, aws.ToString(out.StreamInfo.StreamName))
				assert.EqualValues(t, 48, aws.ToInt32(out.StreamInfo.DataRetentionInHours))
				assert.Equal(t, "video/h264", aws.ToString(out.StreamInfo.MediaType))
				assert.Equal(t, "tf-kvs-device", aws.ToString(out.StreamInfo.DeviceName))

				tagsOut, err := client.ListTagsForStream(ctx, &kinesisvideosdk.ListTagsForStreamInput{
					StreamName: aws.String(streamName),
				})
				require.NoError(t, err, "ListTagsForStream should succeed after terraform apply")
				assert.Equal(t, map[string]string{"Environment": "test", "Owner": "terraform"}, tagsOut.Tags)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}
