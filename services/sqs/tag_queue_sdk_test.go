package sqs_test

import (
	"testing"

	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTagQueueFamily_SDKRoundTrip drives TagQueue, UntagQueue, and
// ListQueueTags through the real aws-sdk-go-v2 client (sqs@v1.46.4,
// JSON-RPC 1.0) instead of hand-constructing JSON bodies, to prove the
// Tags-as-JSON-object wire shape the SDK actually sends decodes correctly
// end to end.
func TestTagQueueFamily_SDKRoundTrip(t *testing.T) {
	t.Parallel()

	client := newTestSQSClientForOversized(t)

	createOut, err := client.CreateQueue(t.Context(), &sqssdk.CreateQueueInput{
		QueueName: new("tagfamily-queue"),
	})
	require.NoError(t, err)

	_, err = client.TagQueue(t.Context(), &sqssdk.TagQueueInput{
		QueueUrl: createOut.QueueUrl,
		Tags:     map[string]string{"env": "prod", "team": "platform"},
	})
	require.NoError(t, err)

	listOut, err := client.ListQueueTags(t.Context(), &sqssdk.ListQueueTagsInput{QueueUrl: createOut.QueueUrl})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod", "team": "platform"}, listOut.Tags)

	_, err = client.UntagQueue(t.Context(), &sqssdk.UntagQueueInput{
		QueueUrl: createOut.QueueUrl,
		TagKeys:  []string{"team"},
	})
	require.NoError(t, err)

	listOut2, err := client.ListQueueTags(t.Context(), &sqssdk.ListQueueTagsInput{QueueUrl: createOut.QueueUrl})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, listOut2.Tags)
}
