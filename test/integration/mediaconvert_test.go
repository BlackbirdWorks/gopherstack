//go:build integration
// +build integration

package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	mediaconvertsdk "github.com/aws/aws-sdk-go-v2/service/mediaconvert"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createMediaConvertClient returns a MediaConvert client pointed at the shared test container.
func createMediaConvertClient(t *testing.T) *mediaconvertsdk.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return mediaconvertsdk.NewFromConfig(cfg, func(o *mediaconvertsdk.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// TestIntegration_MediaConvert_QueueLifecycle tests the full queue CRUD lifecycle.
func TestIntegration_MediaConvert_QueueLifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		queueName string
	}{
		{
			name:      "full_lifecycle",
			queueName: "integration-test-queue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			client := createMediaConvertClient(t)
			queueName := tt.queueName + "-" + t.Name()

			// Create queue.
			createOut, err := client.CreateQueue(ctx, &mediaconvertsdk.CreateQueueInput{
				Name: aws.String(queueName),
			})
			require.NoError(t, err, "CreateQueue should succeed")
			require.NotNil(t, createOut.Queue)
			assert.Equal(t, queueName, aws.ToString(createOut.Queue.Name))
			assert.NotEmpty(t, aws.ToString((*string)(createOut.Queue.Arn)))

			// Get queue.
			getOut, err := client.GetQueue(ctx, &mediaconvertsdk.GetQueueInput{
				Name: aws.String(queueName),
			})
			require.NoError(t, err, "GetQueue should succeed")
			require.NotNil(t, getOut.Queue)
			assert.Equal(t, queueName, aws.ToString(getOut.Queue.Name))

			// List queues — should contain the created one.
			listOut, err := client.ListQueues(ctx, &mediaconvertsdk.ListQueuesInput{})
			require.NoError(t, err, "ListQueues should succeed")

			found := false

			for _, q := range listOut.Queues {
				if aws.ToString(q.Name) == queueName {
					found = true

					break
				}
			}

			assert.True(t, found, "created queue should appear in list")

			// Delete queue.
			_, err = client.DeleteQueue(ctx, &mediaconvertsdk.DeleteQueueInput{
				Name: aws.String(queueName),
			})
			require.NoError(t, err, "DeleteQueue should succeed")

			// Verify deletion.
			_, err = client.GetQueue(ctx, &mediaconvertsdk.GetQueueInput{
				Name: aws.String(queueName),
			})
			require.Error(t, err, "GetQueue after delete should fail")
		})
	}
}
