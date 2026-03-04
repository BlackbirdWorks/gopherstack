package sqs_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/sqs"
)

func makeRedrivePolicy(dlqARN string, maxReceiveCount int) string {
	b, _ := json.Marshal(map[string]any{
		"deadLetterTargetArn": dlqARN,
		"maxReceiveCount":     maxReceiveCount,
	})

	return string(b)
}

// TestRedrivePolicy_DLQMovement merges the "policy set at creation" and
// "policy set via SetQueueAttributes" scenarios into one table-driven test.
func TestRedrivePolicy_DLQMovement(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		maxReceiveCount   int
		msgBody           string
		setPolicyAtCreate bool
	}{
		{
			name:              "policy_set_at_creation",
			maxReceiveCount:   2,
			msgBody:           "hello",
			setPolicyAtCreate: true,
		},
		{
			name:              "policy_set_via_set_queue_attributes",
			maxReceiveCount:   1,
			msgBody:           "test",
			setPolicyAtCreate: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()

			dlqName := "dlq-" + tt.name
			mainName := "main-" + tt.name
			dlqARN := "arn:aws:sqs:us-east-1:000000000000:" + dlqName
			mainURL := "http://localhost/000000000000/" + mainName
			dlqURL := "http://localhost/000000000000/" + dlqName

			_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: dlqName, Endpoint: "localhost"})
			require.NoError(t, err)

			if tt.setPolicyAtCreate {
				_, err = b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: mainName,
					Endpoint:  "localhost",
					Attributes: map[string]string{
						"RedrivePolicy": makeRedrivePolicy(dlqARN, tt.maxReceiveCount),
					},
				})
				require.NoError(t, err)
			} else {
				_, err = b.CreateQueue(&sqs.CreateQueueInput{QueueName: mainName, Endpoint: "localhost"})
				require.NoError(t, err)

				err = b.SetQueueAttributes(&sqs.SetQueueAttributesInput{
					QueueURL: mainURL,
					Attributes: map[string]string{
						"RedrivePolicy": makeRedrivePolicy(dlqARN, tt.maxReceiveCount),
					},
				})
				require.NoError(t, err)
			}

			_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: mainURL, MessageBody: tt.msgBody})
			require.NoError(t, err)

			// Receive maxReceiveCount times — message should still be returned each time.
			for i := range tt.maxReceiveCount {
				out, receiveErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: mainURL, MaxNumberOfMessages: 1})
				require.NoError(t, receiveErr)
				require.Len(t, out.Messages, 1, "receive %d should return message", i+1)

				visErr := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					QueueURL:          mainURL,
					ReceiptHandle:     out.Messages[0].ReceiptHandle,
					VisibilityTimeout: 0,
				})
				require.NoError(t, visErr)
			}

			// Next receive should return empty — message has been moved to DLQ.
			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: mainURL, MaxNumberOfMessages: 1})
			require.NoError(t, err)
			assert.Empty(t, out.Messages, "message should have been moved to DLQ")

			// DLQ should contain the message.
			dlqOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: dlqURL, MaxNumberOfMessages: 1})
			require.NoError(t, err)
			require.Len(t, dlqOut.Messages, 1)
			assert.Equal(t, tt.msgBody, dlqOut.Messages[0].Body)
		})
	}
}

func TestRedrivePolicy_NoMovementWithoutDLQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queueName  string
		msgBody    string
		iterations int
	}{
		{
			name:       "no_redrive_policy",
			queueName:  "plain-queue",
			msgBody:    "stay",
			iterations: 5,
		},
		{
			name:       "high_receive_count_no_dlq",
			queueName:  "plain-queue-high",
			msgBody:    "persistent",
			iterations: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()

			_, err := b.CreateQueue(&sqs.CreateQueueInput{QueueName: tt.queueName, Endpoint: "localhost"})
			require.NoError(t, err)

			qURL := "http://localhost/000000000000/" + tt.queueName

			_, err = b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: tt.msgBody})
			require.NoError(t, err)

			// Receive and re-enqueue iterations times — message must always come back.
			for i := range tt.iterations {
				out, receiveErr := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
				require.NoError(t, receiveErr)
				require.Len(t, out.Messages, 1, "iteration %d", i)

				visErr := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					QueueURL:          qURL,
					ReceiptHandle:     out.Messages[0].ReceiptHandle,
					VisibilityTimeout: 0,
				})
				require.NoError(t, visErr)
			}
		})
	}
}

func TestRedrivePolicy_InvalidJSONIgnored(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		policy    string
		msgBody   string
	}{
		{
			name:    "malformed_json",
			policy:  "{not valid json",
			msgBody: "ok",
		},
		{
			name:    "empty_json_object_no_dlq_fields",
			policy:  "{}",
			msgBody: "also ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sqs.NewInMemoryBackend()

			queueName := "bad-policy-" + tt.name

			require.NotPanics(t, func() {
				_, err := b.CreateQueue(&sqs.CreateQueueInput{
					QueueName: queueName,
					Endpoint:  "localhost",
					Attributes: map[string]string{
						"RedrivePolicy": tt.policy,
					},
				})
				require.NoError(t, err)
			})

			qURL := "http://localhost/000000000000/" + queueName

			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: tt.msgBody})
			require.NoError(t, err)

			out, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{QueueURL: qURL, MaxNumberOfMessages: 1})
			require.NoError(t, err)
			require.Len(t, out.Messages, 1)
			assert.Equal(t, tt.msgBody, out.Messages[0].Body)
		})
	}
}
