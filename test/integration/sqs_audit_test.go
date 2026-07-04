package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_SQSAudit_TagUntagListQueueTags exercises the tag lifecycle
// operations (TagQueue, ListQueueTags, UntagQueue) end-to-end through the AWS
// SDK v2 client. These operations are advertised in both the JSON and Query
// protocols; this test validates the JSON path the SDK uses by default.
func TestIntegration_SQSAudit_TagUntagListQueueTags(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSQSClient(t)
	ctx := t.Context()

	queueName := "audit-tag-queue-" + uuid.NewString()
	createOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.QueueUrl)

	t.Cleanup(func() {
		_, _ = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: createOut.QueueUrl})
	})

	// TagQueue.
	_, err = client.TagQueue(ctx, &sqs.TagQueueInput{
		QueueUrl: createOut.QueueUrl,
		Tags: map[string]string{
			"env":  "prod",
			"team": "platform",
		},
	})
	require.NoError(t, err, "TagQueue should succeed")

	// ListQueueTags should return the tags we set.
	listOut, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{
		QueueUrl: createOut.QueueUrl,
	})
	require.NoError(t, err, "ListQueueTags should succeed")
	assert.Equal(t, map[string]string{"env": "prod", "team": "platform"}, listOut.Tags)

	// UntagQueue should remove the specified key.
	_, err = client.UntagQueue(ctx, &sqs.UntagQueueInput{
		QueueUrl: createOut.QueueUrl,
		TagKeys:  []string{"team"},
	})
	require.NoError(t, err, "UntagQueue should succeed")

	listOut2, err := client.ListQueueTags(ctx, &sqs.ListQueueTagsInput{
		QueueUrl: createOut.QueueUrl,
	})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, listOut2.Tags)
}

// TestIntegration_SQSAudit_ListDeadLetterSourceQueues validates that
// ListDeadLetterSourceQueues returns the source queues that redrive to a DLQ.
func TestIntegration_SQSAudit_ListDeadLetterSourceQueues(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSQSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()

	// Create the dead-letter target queue.
	dlqOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("audit-dlq-" + suffix),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: dlqOut.QueueUrl})
	})

	dlqAttrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       dlqOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	dlqArn := dlqAttrs.Attributes["QueueArn"]
	require.NotEmpty(t, dlqArn)

	// Before any source queue references the DLQ, the list is empty.
	emptyOut, err := client.ListDeadLetterSourceQueues(ctx, &sqs.ListDeadLetterSourceQueuesInput{
		QueueUrl: dlqOut.QueueUrl,
	})
	require.NoError(t, err, "ListDeadLetterSourceQueues should succeed for a DLQ with no sources")
	assert.Empty(t, emptyOut.QueueUrls)

	// Create a source queue with a redrive policy pointing at the DLQ.
	redrivePolicy := `{"deadLetterTargetArn":"` + dlqArn + `","maxReceiveCount":"3"}`
	srcOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("audit-dlq-source-" + suffix),
		Attributes: map[string]string{
			"RedrivePolicy": redrivePolicy,
		},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: srcOut.QueueUrl})
	})

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		out, listErr := client.ListDeadLetterSourceQueues(ctx, &sqs.ListDeadLetterSourceQueuesInput{
			QueueUrl: dlqOut.QueueUrl,
		})
		assert.NoError(c, listErr)
		assert.Contains(c, out.QueueUrls, aws.ToString(srcOut.QueueUrl))
	}, 5*time.Second, 50*time.Millisecond, "source queue should appear in ListDeadLetterSourceQueues")
}

// TestIntegration_SQSAudit_MessageMoveTaskLifecycle validates the message-move
// task operations (StartMessageMoveTask, ListMessageMoveTasks,
// CancelMessageMoveTask) end-to-end.
func TestIntegration_SQSAudit_MessageMoveTaskLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createSQSClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()

	// DLQ (move source) and destination queue.
	dlqOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("audit-move-dlq-" + suffix),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: dlqOut.QueueUrl})
	})

	destOut, err := client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("audit-move-dest-" + suffix),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = client.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: destOut.QueueUrl})
	})

	dlqAttrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       dlqOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	dlqArn := dlqAttrs.Attributes["QueueArn"]
	require.NotEmpty(t, dlqArn)

	destAttrs, err := client.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       destOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	destArn := destAttrs.Attributes["QueueArn"]
	require.NotEmpty(t, destArn)

	// StartMessageMoveTask.
	startOut, err := client.StartMessageMoveTask(ctx, &sqs.StartMessageMoveTaskInput{
		SourceArn:      aws.String(dlqArn),
		DestinationArn: aws.String(destArn),
	})
	require.NoError(t, err, "StartMessageMoveTask should succeed")
	require.NotEmpty(t, aws.ToString(startOut.TaskHandle))

	// ListMessageMoveTasks should report the task for the source.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		listOut, listErr := client.ListMessageMoveTasks(ctx, &sqs.ListMessageMoveTasksInput{
			SourceArn: aws.String(dlqArn),
		})
		assert.NoError(c, listErr)
		assert.NotEmpty(c, listOut.Results)
	}, 5*time.Second, 50*time.Millisecond, "task should appear in ListMessageMoveTasks")

	// CancelMessageMoveTask: the move task races to completion, so cancel either
	// succeeds (still running) or is rejected by AWS for a terminal-state task.
	// Either is fine — we only verify the operation is implemented (not unknown-action).
	_, err = client.CancelMessageMoveTask(ctx, &sqs.CancelMessageMoveTaskInput{
		TaskHandle: startOut.TaskHandle,
	})
	if err != nil {
		require.NotContains(t, err.Error(), "UnknownOperation",
			"CancelMessageMoveTask should be implemented")
		require.NotContains(t, err.Error(), "InvalidAction",
			"CancelMessageMoveTask should be implemented")
	}
}
