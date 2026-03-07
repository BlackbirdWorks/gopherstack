package integration_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_SNS_SQS_CrossService(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	topicName := "test-topic-" + uuid.NewString()[:8]
	queueName := "test-queue-" + uuid.NewString()[:8]

	// Create SNS topic
	topicOut, err := snsClient.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String(topicName)})
	require.NoError(t, err)
	topicArn := *topicOut.TopicArn

	// Create SQS queue
	queueOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{QueueName: aws.String(queueName)})
	require.NoError(t, err)
	queueURL := *queueOut.QueueUrl

	// Get queue ARN
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueArn := attrOut.Attributes["QueueArn"]

	// Subscribe queue to topic
	subOut, err := snsClient.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, subOut.SubscriptionArn)

	// Publish message to topic
	msgBody := `{"test":"message-` + uuid.NewString() + `"}`
	publishOut, err := snsClient.Publish(ctx, &snssdk.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String(msgBody),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, publishOut.MessageId)

	// Receive from SQS — should have the SNS envelope
	recvOut, err := sqsClient.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 5,
		WaitTimeSeconds:     2,
	})
	require.NoError(t, err)
	require.NotEmpty(t, recvOut.Messages, "queue should have received the SNS-published message")

	// Message body should be SNS envelope JSON
	var envelope map[string]any
	require.NoError(t, json.Unmarshal([]byte(*recvOut.Messages[0].Body), &envelope))
	assert.Equal(t, "Notification", envelope["Type"])

	// Cleanup
	_, _ = snsClient.DeleteTopic(ctx, &snssdk.DeleteTopicInput{TopicArn: aws.String(topicArn)})
	_, _ = sqsClient.DeleteQueue(ctx, &sqssdk.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
}

func TestIntegration_SNS_SQS_RawMessageDelivery(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	topicName := "raw-topic-" + uuid.NewString()[:8]
	queueName := "raw-queue-" + uuid.NewString()[:8]

	topicOut, err := snsClient.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String(topicName)})
	require.NoError(t, err)
	topicArn := *topicOut.TopicArn

	queueOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{QueueName: aws.String(queueName)})
	require.NoError(t, err)
	queueURL := *queueOut.QueueUrl

	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueArn := attrOut.Attributes["QueueArn"]

	subOut, err := snsClient.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
	})
	require.NoError(t, err)
	subArn := *subOut.SubscriptionArn

	// Enable raw message delivery.
	_, err = snsClient.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(subArn),
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	require.NoError(t, err)

	// Verify the attribute is persisted.
	getAttrOut, err := snsClient.GetSubscriptionAttributes(ctx, &snssdk.GetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(subArn),
	})
	require.NoError(t, err)
	assert.Equal(t, "true", getAttrOut.Attributes["RawMessageDelivery"])

	msgBody := "raw-payload-" + uuid.NewString()
	_, err = snsClient.Publish(ctx, &snssdk.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String(msgBody),
	})
	require.NoError(t, err)

	recvOut, err := sqsClient.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     2,
	})
	require.NoError(t, err)
	require.NotEmpty(t, recvOut.Messages, "queue should have received the raw message")

	// With RawMessageDelivery=true the body is the plain message, not an SNS envelope.
	assert.Equal(t, msgBody, *recvOut.Messages[0].Body)

	// Cleanup
	_, _ = snsClient.DeleteTopic(ctx, &snssdk.DeleteTopicInput{TopicArn: aws.String(topicArn)})
	_, _ = sqsClient.DeleteQueue(ctx, &sqssdk.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
}

func TestIntegration_SNS_SQS_RawMessageDelivery_WithAttributes(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	topicName := "raw-attr-topic-" + uuid.NewString()[:8]
	queueName := "raw-attr-queue-" + uuid.NewString()[:8]

	topicOut, err := snsClient.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String(topicName)})
	require.NoError(t, err)
	topicArn := *topicOut.TopicArn

	queueOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{QueueName: aws.String(queueName)})
	require.NoError(t, err)
	queueURL := *queueOut.QueueUrl

	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueArn := attrOut.Attributes["QueueArn"]

	subOut, err := snsClient.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
	})
	require.NoError(t, err)
	subArn := *subOut.SubscriptionArn

	_, err = snsClient.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(subArn),
		AttributeName:   aws.String("RawMessageDelivery"),
		AttributeValue:  aws.String("true"),
	})
	require.NoError(t, err)

	_, err = snsClient.Publish(ctx, &snssdk.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String("payload"),
		MessageAttributes: map[string]snstypes.MessageAttributeValue{
			"event-type": {
				DataType:    aws.String("String"),
				StringValue: aws.String("order.created"),
			},
		},
	})
	require.NoError(t, err)

	recvOut, err := sqsClient.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		MaxNumberOfMessages:   1,
		WaitTimeSeconds:       2,
		MessageAttributeNames: []string{"All"},
	})
	require.NoError(t, err)
	require.NotEmpty(t, recvOut.Messages, "queue should have received the raw message with attributes")

	assert.Equal(t, "payload", *recvOut.Messages[0].Body)
	attr, ok := recvOut.Messages[0].MessageAttributes["event-type"]
	require.True(t, ok, "SQS message should carry mapped SNS message attributes")
	assert.Equal(t, "order.created", *attr.StringValue)

	_, _ = snsClient.DeleteTopic(ctx, &snssdk.DeleteTopicInput{TopicArn: aws.String(topicArn)})
	_, _ = sqsClient.DeleteQueue(ctx, &sqssdk.DeleteQueueInput{QueueUrl: aws.String(queueURL)})
}

func TestIntegration_SNS_SQS_RedrivePolicy(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	topicName := "rdq-topic-" + uuid.NewString()[:8]
	dlqName := "rdq-dlq-" + uuid.NewString()[:8]

	topicOut, err := snsClient.CreateTopic(ctx, &snssdk.CreateTopicInput{Name: aws.String(topicName)})
	require.NoError(t, err)
	topicArn := *topicOut.TopicArn

	dlqOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{QueueName: aws.String(dlqName)})
	require.NoError(t, err)
	dlqURL := *dlqOut.QueueUrl

	dlqAttrOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       aws.String(dlqURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	dlqArn := dlqAttrOut.Attributes["QueueArn"]

	// Subscribe to a non-existent queue so delivery will fail.
	subOut, err := snsClient.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String("arn:aws:sqs:us-east-1:000000000000:nonexistent-queue-" + uuid.NewString()[:8]),
	})
	require.NoError(t, err)
	subArn := *subOut.SubscriptionArn

	redrivePolicy := `{"deadLetterTargetArn":"` + dlqArn + `"}`
	_, err = snsClient.SetSubscriptionAttributes(ctx, &snssdk.SetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(subArn),
		AttributeName:   aws.String("RedrivePolicy"),
		AttributeValue:  aws.String(redrivePolicy),
	})
	require.NoError(t, err)

	// Verify the attribute is persisted.
	getAttrOut, err := snsClient.GetSubscriptionAttributes(ctx, &snssdk.GetSubscriptionAttributesInput{
		SubscriptionArn: aws.String(subArn),
	})
	require.NoError(t, err)
	assert.Equal(t, redrivePolicy, getAttrOut.Attributes["RedrivePolicy"])

	msgBody := "dlq-message-" + uuid.NewString()
	_, err = snsClient.Publish(ctx, &snssdk.PublishInput{
		TopicArn: aws.String(topicArn),
		Message:  aws.String(msgBody),
	})
	require.NoError(t, err)

	recvOut, err := sqsClient.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{
		QueueUrl:            aws.String(dlqURL),
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     2,
	})
	require.NoError(t, err)
	require.NotEmpty(t, recvOut.Messages, "DLQ should have received the failed message")
	// DLQ receives the same body that was attempted to the failed queue.
	// Without RawMessageDelivery=true, that body is the SNS envelope JSON.
	var dlqEnvelope map[string]any
	require.NoError(t, json.Unmarshal([]byte(*recvOut.Messages[0].Body), &dlqEnvelope))
	assert.Equal(t, "Notification", dlqEnvelope["Type"])
	assert.Equal(t, msgBody, dlqEnvelope["Message"])

	_, _ = snsClient.DeleteTopic(ctx, &snssdk.DeleteTopicInput{TopicArn: aws.String(topicArn)})
	_, _ = sqsClient.DeleteQueue(ctx, &sqssdk.DeleteQueueInput{QueueUrl: aws.String(dlqURL)})
}
