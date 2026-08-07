package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_SQS_CreateQueueIdempotent verifies the AWS idempotency rule:
// recreating a queue with the same (or no) attributes succeeds; recreating with
// different attributes returns QueueNameExists.
func TestIntegration_SQS_CreateQueueIdempotent(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	tests := []struct {
		firstAttrs  map[string]string
		secondAttrs map[string]string
		name        string
		wantErr     bool
	}{
		{
			name:        "same_name_no_attrs",
			firstAttrs:  nil,
			secondAttrs: nil,
			wantErr:     false,
		},
		{
			name:       "same_name_same_attrs",
			firstAttrs: map[string]string{"VisibilityTimeout": "30"},
			secondAttrs: map[string]string{
				"VisibilityTimeout": "30",
			},
			wantErr: false,
		},
		{
			name:        "same_name_diff_attrs",
			firstAttrs:  nil,
			secondAttrs: map[string]string{"VisibilityTimeout": "60"},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dumpContainerLogsOnFailure(t)

			qName := "idem-" + tt.name + "-" + uuid.NewString()[:8]

			_, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName:  aws.String(qName),
				Attributes: tt.firstAttrs,
			})
			require.NoError(t, err)

			_, err = client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName:  aws.String(qName),
				Attributes: tt.secondAttrs,
			})
			if tt.wantErr {
				require.Error(t, err)
				var queueNameExists *sqstypes.QueueNameExists
				assert.ErrorAs(t, err, &queueNameExists, "expected QueueNameExists error")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestIntegration_SQS_SetQueueAttributes verifies that SetQueueAttributes
// updates the queue's attributes and they are visible via GetQueueAttributes.
func TestIntegration_SQS_SetQueueAttributes(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	qName := "set-attrs-" + uuid.NewString()[:8]
	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(qName),
	})
	require.NoError(t, err)

	qURL := *out.QueueUrl
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(qURL)})
	})

	_, err = client.SetQueueAttributes(ctx, &sqs.SetQueueAttributesInput{
		QueueUrl: aws.String(qURL),
		Attributes: map[string]string{
			"VisibilityTimeout":      "120",
			"MessageRetentionPeriod": "7200",
		},
	})
	require.NoError(t, err)

	attrsOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(qURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameAll},
	})
	require.NoError(t, err)
	assert.Equal(t, "120", attrsOut.Attributes["VisibilityTimeout"])
	assert.Equal(t, "7200", attrsOut.Attributes["MessageRetentionPeriod"])
}

// TestIntegration_SQS_MessageAttributesFilter verifies that
// MessageAttributeNames correctly filters which attributes are returned.
func TestIntegration_SQS_MessageAttributesFilter(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	tests := []struct {
		name             string
		filter           []string
		wantAttrNames    []string
		wantMissingNames []string
	}{
		{
			name:          "all",
			filter:        []string{"All"},
			wantAttrNames: []string{"Color", "Count"},
		},
		{
			name:             "only_color",
			filter:           []string{"Color"},
			wantAttrNames:    []string{"Color"},
			wantMissingNames: []string{"Count"},
		},
		{
			name:             "no_filter_returns_none",
			filter:           nil,
			wantMissingNames: []string{"Color", "Count"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dumpContainerLogsOnFailure(t)

			// Each sub-test owns its own queue to avoid PurgeQueue cooldown conflicts.
			qName := "msg-attr-filter-" + uuid.NewString()[:8]
			qOut, createErr := client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(qName),
			})
			require.NoError(t, createErr)

			qURL := *qOut.QueueUrl
			t.Cleanup(func() {
				cleanupCtx, cancel := cleanupContext(t)
				defer cancel()

				_, _ = client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(qURL)})
			})

			_, sendErr := client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:    aws.String(qURL),
				MessageBody: aws.String("hello"),
				MessageAttributes: map[string]sqstypes.MessageAttributeValue{
					"Color": {
						DataType:    aws.String("String"),
						StringValue: aws.String("blue"),
					},
					"Count": {
						DataType:    aws.String("Number"),
						StringValue: aws.String("42"),
					},
				},
			})
			require.NoError(t, sendErr)

			recv, recvErr := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:              aws.String(qURL),
				MaxNumberOfMessages:   1,
				MessageAttributeNames: tt.filter,
			})
			require.NoError(t, recvErr)
			require.Len(t, recv.Messages, 1)

			for _, wantName := range tt.wantAttrNames {
				assert.Contains(t, recv.Messages[0].MessageAttributes, wantName,
					"expected attribute %q to be present", wantName)
			}

			for _, missingName := range tt.wantMissingNames {
				assert.NotContains(t, recv.Messages[0].MessageAttributes, missingName,
					"expected attribute %q to be absent", missingName)
			}
		})
	}
}

// TestIntegration_SQS_QueueTags verifies that TagQueue/UntagQueue/ListQueueTags
// round-trip correctly.
func TestIntegration_SQS_QueueTags(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	qName := "tags-" + uuid.NewString()[:8]
	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(qName),
	})
	require.NoError(t, err)

	qURL := *out.QueueUrl
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(qURL)})
	})

	// Tag the queue.
	_, err = client.TagQueue(ctx, &sqs.TagQueueInput{
		QueueUrl: aws.String(qURL),
		Tags: map[string]string{
			"env":   "test",
			"owner": "team-a",
		},
	})
	require.NoError(t, err)

	// Verify tags.
	listOut, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{
		QueueUrl: aws.String(qURL),
	})
	require.NoError(t, err)
	assert.Equal(t, "test", listOut.Tags["env"])
	assert.Equal(t, "team-a", listOut.Tags["owner"])

	// Remove one tag.
	_, err = client.UntagQueue(ctx, &sqs.UntagQueueInput{
		QueueUrl: aws.String(qURL),
		TagKeys:  []string{"owner"},
	})
	require.NoError(t, err)

	// Verify removal.
	listOut2, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{
		QueueUrl: aws.String(qURL),
	})
	require.NoError(t, err)
	assert.Equal(t, "test", listOut2.Tags["env"])
	assert.NotContains(t, listOut2.Tags, "owner")
}

// TestIntegration_SQS_ApproxMessagesDelayed verifies that
// ApproximateNumberOfMessagesDelayed reflects delayed messages.
func TestIntegration_SQS_ApproxMessagesDelayed(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	qName := "delayed-count-" + uuid.NewString()[:8]
	out, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(qName),
	})
	require.NoError(t, err)

	qURL := *out.QueueUrl
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteQueue(cleanupCtx, &sqs.DeleteQueueInput{QueueUrl: aws.String(qURL)})
	})

	// Send 2 delayed messages (900 seconds delay).
	for range 2 {
		_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:     aws.String(qURL),
			MessageBody:  aws.String("delayed"),
			DelaySeconds: 900,
		})
		require.NoError(t, err)
	}

	attrsOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(qURL),
		AttributeNames: []sqstypes.QueueAttributeName{
			sqstypes.QueueAttributeNameApproximateNumberOfMessagesDelayed,
			sqstypes.QueueAttributeNameApproximateNumberOfMessages,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "2", attrsOut.Attributes["ApproximateNumberOfMessagesDelayed"])
	assert.Equal(t, "0", attrsOut.Attributes["ApproximateNumberOfMessages"])
}

// TestIntegration_SQS_QueueNameValidation verifies that invalid queue names
// are rejected at creation time.
func TestIntegration_SQS_QueueNameValidation(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	tests := []struct {
		name    string
		qName   string
		wantErr bool
	}{
		{name: "valid_name", qName: "valid-queue-" + uuid.NewString()[:8], wantErr: false},
		{name: "empty_name", qName: "", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dumpContainerLogsOnFailure(t)

			_, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(tt.qName),
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
