package integration_test

import (
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_SQS_DLQ tests that messages exceeding maxReceiveCount are
// routed to the dead-letter queue instead of being returned to consumers.
func TestIntegration_SQS_DLQ(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	tests := []struct {
		name            string
		msgBody         string
		maxReceiveCount int
	}{
		{
			name:            "dlq_after_one_receive",
			maxReceiveCount: 1,
			msgBody:         "redrive-body-1",
		},
		{
			name:            "dlq_after_two_receives",
			maxReceiveCount: 2,
			msgBody:         "redrive-body-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dumpContainerLogsOnFailure(t)

			suffix := uuid.NewString()
			dlqName := "dlq-" + tt.name + "-" + suffix
			mainName := "main-" + tt.name + "-" + suffix

			// Create DLQ.
			dlqOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(dlqName),
			})
			require.NoError(t, err)

			// Fetch DLQ ARN.
			dlqAttrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
				QueueUrl:       dlqOut.QueueUrl,
				AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
			})
			require.NoError(t, err)
			dlqARN := dlqAttrs.Attributes["QueueArn"]

			// Build redrive policy JSON.
			redrivePolicy := `{"deadLetterTargetArn":"` + dlqARN +
				`","maxReceiveCount":` + strconv.Itoa(tt.maxReceiveCount) + `}`

			// Create main queue with redrive policy.
			mainOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(mainName),
				Attributes: map[string]string{
					"RedrivePolicy":     redrivePolicy,
					"VisibilityTimeout": "0",
				},
			})
			require.NoError(t, err)
			mainURL := mainOut.QueueUrl

			// Send a message.
			_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:    mainURL,
				MessageBody: aws.String(tt.msgBody),
			})
			require.NoError(t, err)

			// Receive maxReceiveCount times — each time the message should be returned.
			for i := range tt.maxReceiveCount {
				recvOut, receiveErr := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
					QueueUrl:            mainURL,
					MaxNumberOfMessages: 1,
					WaitTimeSeconds:     0,
				})
				require.NoError(t, receiveErr)
				require.Len(t, recvOut.Messages, 1, "receive %d should return the message", i+1)

				// Change visibility to 0 so it becomes available again immediately.
				_, visErr := client.ChangeMessageVisibility(ctx, &sqs.ChangeMessageVisibilityInput{
					QueueUrl:          mainURL,
					ReceiptHandle:     recvOut.Messages[0].ReceiptHandle,
					VisibilityTimeout: 0,
				})
				require.NoError(t, visErr)
			}

			// The next receive should be empty — message moved to DLQ.
			recvEmpty, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            mainURL,
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     0,
			})
			require.NoError(t, err)
			assert.Empty(t, recvEmpty.Messages, "main queue should be empty after DLQ routing")

			// DLQ should contain the message.
			dlqRecv, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            dlqOut.QueueUrl,
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     0,
			})
			require.NoError(t, err)
			require.Len(t, dlqRecv.Messages, 1, "DLQ should have the dead-lettered message")
			assert.Equal(t, tt.msgBody, aws.ToString(dlqRecv.Messages[0].Body))
		})
	}
}

// TestIntegration_SQS_DelayQueue verifies that messages with DelaySeconds are
// hidden until the delay window elapses.
func TestIntegration_SQS_DelayQueue(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	tests := []struct {
		name         string
		waitBefore   time.Duration
		delaySeconds int32
		wantVisible  bool
	}{
		{
			name:         "message_hidden_during_delay",
			delaySeconds: 5,
			waitBefore:   0,
			wantVisible:  false,
		},
		{
			name:         "message_visible_after_delay",
			delaySeconds: 1,
			waitBefore:   2 * time.Second,
			wantVisible:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dumpContainerLogsOnFailure(t)

			queueName := "delay-" + tt.name + "-" + uuid.NewString()
			createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(queueName),
			})
			require.NoError(t, err)
			queueURL := createOut.QueueUrl

			_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:     queueURL,
				MessageBody:  aws.String("delayed-message"),
				DelaySeconds: tt.delaySeconds,
			})
			require.NoError(t, err)

			if tt.waitBefore > 0 {
				time.Sleep(tt.waitBefore)
			}

			recvOut, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            queueURL,
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     0,
			})
			require.NoError(t, err)

			if tt.wantVisible {
				require.Len(t, recvOut.Messages, 1, "message should be visible after delay")
				assert.Equal(t, "delayed-message", aws.ToString(recvOut.Messages[0].Body))
			} else {
				assert.Empty(t, recvOut.Messages, "message should be hidden during delay")
			}
		})
	}
}

// TestIntegration_SQS_QueueLevelDelay tests that the queue's own DelaySeconds
// attribute causes all messages to be hidden on send.
func TestIntegration_SQS_QueueLevelDelay(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	tests := []struct {
		name           string
		queueDelaySecs string
		waitBefore     time.Duration
		wantVisible    bool
	}{
		{
			name:           "queue_delay_hides_message",
			queueDelaySecs: "5",
			waitBefore:     0,
			wantVisible:    false,
		},
		{
			name:           "queue_delay_message_becomes_visible",
			queueDelaySecs: "1",
			waitBefore:     2 * time.Second,
			wantVisible:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dumpContainerLogsOnFailure(t)

			queueName := "qdel-" + tt.name + "-" + uuid.NewString()
			createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(queueName),
				Attributes: map[string]string{
					"DelaySeconds": tt.queueDelaySecs,
				},
			})
			require.NoError(t, err)
			queueURL := createOut.QueueUrl

			_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:    queueURL,
				MessageBody: aws.String("body"),
			})
			require.NoError(t, err)

			if tt.waitBefore > 0 {
				time.Sleep(tt.waitBefore)
			}

			recvOut, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            queueURL,
				MaxNumberOfMessages: 1,
				WaitTimeSeconds:     0,
			})
			require.NoError(t, err)

			if tt.wantVisible {
				require.Len(t, recvOut.Messages, 1, "message should be visible")
			} else {
				assert.Empty(t, recvOut.Messages, "message should still be hidden")
			}
		})
	}
}

// TestIntegration_SQS_FIFODeduplication tests that FIFO queues deduplicate
// messages with the same MessageDeduplicationId within the 5-minute window.
func TestIntegration_SQS_FIFODeduplication(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createSQSClient(t)
	ctx := t.Context()

	tests := []struct {
		name       string
		firstBody  string
		secondBody string
		dedupID    string
		wantCount  int
	}{
		{
			name:       "duplicate_within_window_dropped",
			firstBody:  "body-a",
			secondBody: "body-b",
			dedupID:    uuid.NewString(),
			wantCount:  1, // second send is dropped
		},
		{
			name:       "different_dedup_ids_both_delivered",
			firstBody:  "msg1",
			secondBody: "msg2",
			dedupID:    "", // use distinct IDs per send
			wantCount:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dumpContainerLogsOnFailure(t)

			queueName := "fifo-dedup-" + tt.name + "-" + uuid.NewString() + ".fifo"
			createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(queueName),
				Attributes: map[string]string{
					"FifoQueue": "true",
				},
			})
			require.NoError(t, err)
			queueURL := createOut.QueueUrl

			// Determine dedup IDs for both sends.
			firstDedupID := tt.dedupID
			if firstDedupID == "" {
				firstDedupID = uuid.NewString()
			}

			secondDedupID := tt.dedupID
			if secondDedupID == "" {
				secondDedupID = uuid.NewString()
			}

			groupID := aws.String("group-" + uuid.NewString())

			_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:               queueURL,
				MessageBody:            aws.String(tt.firstBody),
				MessageGroupId:         groupID,
				MessageDeduplicationId: aws.String(firstDedupID),
			})
			require.NoError(t, err)

			_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
				QueueUrl:               queueURL,
				MessageBody:            aws.String(tt.secondBody),
				MessageGroupId:         groupID,
				MessageDeduplicationId: aws.String(secondDedupID),
			})
			require.NoError(t, err)

			// Receive up to 10 messages to count what's actually in the queue.
			recvOut, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
				QueueUrl:            queueURL,
				MaxNumberOfMessages: 10,
				WaitTimeSeconds:     0,
			})
			require.NoError(t, err)
			assert.Len(t, recvOut.Messages, tt.wantCount,
				"unexpected number of messages delivered")
		})
	}
}
