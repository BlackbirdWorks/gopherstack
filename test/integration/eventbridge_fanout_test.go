package integration_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	eventbridgesdk "github.com/aws/aws-sdk-go-v2/service/eventbridge"
	ebtypes "github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_EventBridge_FanoutToSQS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ebClient := createEventBridgeClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	busName := "fanout-bus-" + uuid.NewString()[:8]
	ruleName := "fanout-rule-" + uuid.NewString()[:8]
	queueName := "fanout-queue-" + uuid.NewString()[:8]

	// Create SQS queue.
	queueOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = sqsClient.DeleteQueue(ctx, &sqssdk.DeleteQueueInput{QueueUrl: queueOut.QueueUrl})
	})

	// Get queue ARN.
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       queueOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	// Create EventBridge event bus.
	_, err = ebClient.CreateEventBus(ctx, &eventbridgesdk.CreateEventBusInput{
		Name: aws.String(busName),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = ebClient.DeleteEventBus(ctx, &eventbridgesdk.DeleteEventBusInput{Name: aws.String(busName)})
	})

	// Create rule with event pattern.
	_, err = ebClient.PutRule(ctx, &eventbridgesdk.PutRuleInput{
		Name:         aws.String(ruleName),
		EventBusName: aws.String(busName),
		EventPattern: aws.String(`{"source": ["integration.test"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = ebClient.DeleteRule(ctx, &eventbridgesdk.DeleteRuleInput{
			Name:         aws.String(ruleName),
			EventBusName: aws.String(busName),
		})
	})

	// Add SQS target.
	targetsOut, err := ebClient.PutTargets(ctx, &eventbridgesdk.PutTargetsInput{
		Rule:         aws.String(ruleName),
		EventBusName: aws.String(busName),
		Targets: []ebtypes.Target{
			{Id: aws.String("t1"), Arn: aws.String(queueARN)},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), targetsOut.FailedEntryCount)

	// Put a matching event.
	putOut, err := ebClient.PutEvents(ctx, &eventbridgesdk.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{
				Source:       aws.String("integration.test"),
				DetailType:   aws.String("TestEvent"),
				Detail:       aws.String(`{"key": "value"}`),
				EventBusName: aws.String(busName),
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), putOut.FailedEntryCount)

	// Poll SQS until message arrives (fan-out is async).
	var received string
	require.Eventually(t, func() bool {
		msgs, recvErr := sqsClient.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{
			QueueUrl:            queueOut.QueueUrl,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		})
		if recvErr != nil || len(msgs.Messages) == 0 {
			return false
		}
		received = aws.ToString(msgs.Messages[0].Body)

		return true
	}, 10*time.Second, 500*time.Millisecond, "expected SQS message from EventBridge fan-out")

	assert.Contains(t, received, "integration.test")
}

func TestIntegration_EventBridge_FanoutNoMatch(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ebClient := createEventBridgeClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	busName := "nomatch-bus-" + uuid.NewString()[:8]
	ruleName := "nomatch-rule-" + uuid.NewString()[:8]
	queueName := "nomatch-queue-" + uuid.NewString()[:8]

	queueOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = sqsClient.DeleteQueue(ctx, &sqssdk.DeleteQueueInput{QueueUrl: queueOut.QueueUrl})
	})

	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       queueOut.QueueUrl,
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	_, err = ebClient.CreateEventBus(ctx, &eventbridgesdk.CreateEventBusInput{Name: aws.String(busName)})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = ebClient.DeleteEventBus(ctx, &eventbridgesdk.DeleteEventBusInput{Name: aws.String(busName)})
	})

	// Rule only matches "other.source", not "integration.test".
	_, err = ebClient.PutRule(ctx, &eventbridgesdk.PutRuleInput{
		Name:         aws.String(ruleName),
		EventBusName: aws.String(busName),
		EventPattern: aws.String(`{"source": ["other.source"]}`),
		State:        ebtypes.RuleStateEnabled,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = ebClient.DeleteRule(ctx, &eventbridgesdk.DeleteRuleInput{
			Name:         aws.String(ruleName),
			EventBusName: aws.String(busName),
		})
	})

	_, err = ebClient.PutTargets(ctx, &eventbridgesdk.PutTargetsInput{
		Rule:         aws.String(ruleName),
		EventBusName: aws.String(busName),
		Targets:      []ebtypes.Target{{Id: aws.String("t1"), Arn: aws.String(queueARN)}},
	})
	require.NoError(t, err)

	// Put an event from a non-matching source.
	_, err = ebClient.PutEvents(ctx, &eventbridgesdk.PutEventsInput{
		Entries: []ebtypes.PutEventsRequestEntry{
			{
				Source:       aws.String("integration.test"),
				DetailType:   aws.String("TestEvent"),
				Detail:       aws.String(`{}`),
				EventBusName: aws.String(busName),
			},
		},
	})
	require.NoError(t, err)

	// Wait briefly and verify no messages arrived.
	time.Sleep(500 * time.Millisecond)

	msgs, err := sqsClient.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{
		QueueUrl:            queueOut.QueueUrl,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     1,
	})
	require.NoError(t, err)
	assert.Empty(t, msgs.Messages, "expected no messages for non-matching event pattern")
}
