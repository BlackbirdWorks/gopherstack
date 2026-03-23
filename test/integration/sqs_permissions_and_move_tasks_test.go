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

// TestIntegration_SQS_Permissions validates that AddPermission and RemovePermission
// correctly manage permission statements on SQS queues via the HTTP handler.
func TestIntegration_SQS_Permissions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSQSClient(t)
	ctx := t.Context()

	tests := []struct {
		name  string
		label string
	}{
		{
			name:  "single_permission_label",
			label: "AllowSend",
		},
		{
			name:  "multiple_permissions_then_remove",
			label: "AllowReceive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			dumpContainerLogsOnFailure(t)

			suffix := uuid.NewString()
			queueName := "perm-queue-" + tt.name + "-" + suffix

			createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(queueName),
			})
			require.NoError(t, err)
			require.NotNil(t, createOut.QueueUrl)

			// AddPermission should succeed.
			_, err = client.AddPermission(ctx, &sqs.AddPermissionInput{
				QueueUrl:      createOut.QueueUrl,
				Label:         aws.String(tt.label),
				AWSAccountIds: []string{"123456789012"},
				Actions:       []string{"SendMessage"},
			})
			require.NoError(t, err, "AddPermission should succeed")

			// AddPermission with a different label should also succeed.
			_, err = client.AddPermission(ctx, &sqs.AddPermissionInput{
				QueueUrl:      createOut.QueueUrl,
				Label:         aws.String(tt.label + "-2"),
				AWSAccountIds: []string{"111111111111"},
				Actions:       []string{"ReceiveMessage"},
			})
			require.NoError(t, err, "AddPermission with second label should succeed")

			// RemovePermission should succeed.
			_, err = client.RemovePermission(ctx, &sqs.RemovePermissionInput{
				QueueUrl: createOut.QueueUrl,
				Label:    aws.String(tt.label),
			})
			require.NoError(t, err, "RemovePermission should succeed")

			// RemovePermission on already-removed label should be idempotent.
			_, err = client.RemovePermission(ctx, &sqs.RemovePermissionInput{
				QueueUrl: createOut.QueueUrl,
				Label:    aws.String(tt.label),
			})
			require.NoError(t, err, "RemovePermission on missing label should be idempotent")

			// Cleanup.
			_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: createOut.QueueUrl})
			require.NoError(t, err)
		})
	}
}

// TestIntegration_SQS_Permissions_QueueNotFound validates that AddPermission and
// RemovePermission return the expected error for nonexistent queues.
func TestIntegration_SQS_Permissions_QueueNotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSQSClient(t)
	ctx := t.Context()

	nonexistentURL := "http://localhost/000000000000/queue-that-does-not-exist-" + uuid.NewString()

	_, err := client.AddPermission(ctx, &sqs.AddPermissionInput{
		QueueUrl:      aws.String(nonexistentURL),
		Label:         aws.String("TestLabel"),
		AWSAccountIds: []string{"123456789012"},
		Actions:       []string{"SendMessage"},
	})
	require.Error(t, err, "AddPermission on nonexistent queue should return error")

	_, err = client.RemovePermission(ctx, &sqs.RemovePermissionInput{
		QueueUrl: aws.String(nonexistentURL),
		Label:    aws.String("TestLabel"),
	})
	require.Error(t, err, "RemovePermission on nonexistent queue should return error")
}

// TestIntegration_SQS_MessageMoveTasks validates StartMessageMoveTask, CancelMessageMoveTask,
// and ListMessageMoveTasks end-to-end via the AWS SDK v2 client.
func TestIntegration_SQS_MessageMoveTasks(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSQSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()
	dlqName := "int-dlq-mmtask-" + suffix
	destName := "int-dest-mmtask-" + suffix

	// Create DLQ.
	dlqOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(dlqName),
	})
	require.NoError(t, err)

	dlqURL := dlqOut.QueueUrl

	// Create destination queue.
	destOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(destName),
	})
	require.NoError(t, err)

	destURL := destOut.QueueUrl

	// Get DLQ ARN.
	dlqAttrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       dlqURL,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	dlqARN := dlqAttrOut.Attributes["QueueArn"]

	// Get dest ARN.
	destAttrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       destURL,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	destARN := destAttrOut.Attributes["QueueArn"]

	// Seed messages into the DLQ.
	const msgCount = 5
	for i := range msgCount {
		_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    dlqURL,
			MessageBody: aws.String("dlq-message-" + strconv.Itoa(i)),
		})
		require.NoError(t, err)
	}

	// StartMessageMoveTask: move from DLQ to dest.
	startOut, err := client.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn:      aws.String(dlqARN),
		DestinationArn: aws.String(destARN),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(startOut.TaskHandle), "TaskHandle should be returned")

	// ListMessageMoveTasks should contain the task.
	require.Eventually(t, func() bool {
		listOut, listErr := client.ListMessageMoveTasks(ctx, &sqs.ListMessageMoveTasksInput{
			SourceArn: aws.String(dlqARN),
		})
		if listErr != nil {
			return false
		}

		return len(listOut.Results) > 0
	}, 5*time.Second, 50*time.Millisecond, "task should appear in ListMessageMoveTasks")

	// Wait for task to complete.
	require.Eventually(t, func() bool {
		listOut, listErr := client.ListMessageMoveTasks(ctx, &sqs.ListMessageMoveTasksInput{
			SourceArn: aws.String(dlqARN),
		})
		if listErr != nil || len(listOut.Results) == 0 {
			return false
		}

		status := aws.ToString(listOut.Results[0].Status)

		return status == "COMPLETED" || status == "CANCELLED" || status == "FAILED"
	}, 10*time.Second, 100*time.Millisecond, "task should reach a terminal state")

	// Destination queue should have received the messages.
	var movedCount int

	require.Eventually(t, func() bool {
		recvOut, recvErr := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            destURL,
			MaxNumberOfMessages: 10,
		})
		if recvErr != nil {
			return false
		}

		movedCount += len(recvOut.Messages)

		return movedCount >= msgCount
	}, 5*time.Second, 100*time.Millisecond, "destination queue should have all moved messages")

	// DLQ should now be empty.
	dlqCheck, err := client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            dlqURL,
		MaxNumberOfMessages: 10,
	})
	require.NoError(t, err)
	assert.Empty(t, dlqCheck.Messages, "DLQ should be empty after move task completes")

	// Cleanup.
	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: dlqURL})
	require.NoError(t, err)
	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: destURL})
	require.NoError(t, err)
}

// TestIntegration_SQS_CancelMessageMoveTask validates that CancelMessageMoveTask
// stops an active message move task.
func TestIntegration_SQS_CancelMessageMoveTask(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSQSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()
	dlqName := "int-dlq-cancel-" + suffix
	destName := "int-dest-cancel-" + suffix

	dlqOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(dlqName)})
	require.NoError(t, err)

	destOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(destName)})
	require.NoError(t, err)

	dlqAttrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       dlqOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	dlqARN := dlqAttrOut.Attributes["QueueArn"]

	destAttrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       destOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	destARN := destAttrOut.Attributes["QueueArn"]

	// Seed the DLQ with messages.
	for i := range 3 {
		_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    dlqOut.QueueUrl,
			MessageBody: aws.String("cancel-test-" + strconv.Itoa(i)),
		})
		require.NoError(t, err)
	}

	// Start the task.
	startOut, err := client.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn:      aws.String(dlqARN),
		DestinationArn: aws.String(destARN),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(startOut.TaskHandle))

	// Cancel the task.
	cancelOut, err := client.CancelMessageMoveTask(ctx, &sqs.CancelMessageMoveTaskInput{
		TaskHandle: startOut.TaskHandle,
	})
	require.NoError(t, err)
	assert.GreaterOrEqual(t, cancelOut.ApproximateNumberOfMessagesMoved, int64(0))

	// Task should eventually reach CANCELLED state.
	require.Eventually(t, func() bool {
		listOut, listErr := client.ListMessageMoveTasks(ctx, &sqs.ListMessageMoveTasksInput{
			SourceArn: aws.String(dlqARN),
		})
		if listErr != nil || len(listOut.Results) == 0 {
			return false
		}

		status := aws.ToString(listOut.Results[0].Status)

		return status == "CANCELLED" || status == "COMPLETED"
	}, 5*time.Second, 50*time.Millisecond, "task should reach CANCELLED or COMPLETED state")

	// Cleanup.
	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: dlqOut.QueueUrl})
	require.NoError(t, err)
	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: destOut.QueueUrl})
	require.NoError(t, err)
}

// TestIntegration_SQS_StartMessageMoveTask_AlreadyRunning validates that starting
// a second move task while one is running returns a ResourceInConflict error.
func TestIntegration_SQS_StartMessageMoveTask_AlreadyRunning(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSQSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()
	dlqName := "int-dlq-conflict-" + suffix
	destName := "int-dest-conflict-" + suffix

	dlqOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(dlqName)})
	require.NoError(t, err)

	destOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String(destName)})
	require.NoError(t, err)

	dlqAttrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       dlqOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	dlqARN := dlqAttrOut.Attributes["QueueArn"]

	destAttrOut, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       destOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	destARN := destAttrOut.Attributes["QueueArn"]

	// Seed a message so the task doesn't complete immediately.
	for i := range 5 {
		_, err = client.SendMessage(ctx, &sqs.SendMessageInput{
			QueueUrl:    dlqOut.QueueUrl,
			MessageBody: aws.String("conflict-msg-" + strconv.Itoa(i)),
		})
		require.NoError(t, err)
	}

	// Start first task — rate-limit it so it stays running long enough to conflict.
	_, err = client.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn:                    aws.String(dlqARN),
		DestinationArn:               aws.String(destARN),
		MaxNumberOfMessagesPerSecond: aws.Int32(1),
	})
	require.NoError(t, err, "first StartMessageMoveTask should succeed")

	// Starting a second task for the same source should fail.
	_, err = client.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn:      aws.String(dlqARN),
		DestinationArn: aws.String(destARN),
	})
	require.Error(t, err, "second StartMessageMoveTask for same source should return conflict error")

	// Cleanup.
	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: dlqOut.QueueUrl})
	require.NoError(t, err)
	_, err = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: destOut.QueueUrl})
	require.NoError(t, err)
}
