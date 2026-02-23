package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sns"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_SNS_TopicLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	ctx := t.Context()

	topicName := "test-topic-" + uuid.NewString()

	// CreateTopic
	createOut, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.TopicArn)
	assert.Contains(t, *createOut.TopicArn, topicName)

	topicARN := createOut.TopicArn

	// ListTopics
	listOut, err := snsClient.ListTopics(ctx, &sns.ListTopicsInput{})
	require.NoError(t, err)
	found := false
	for _, t2 := range listOut.Topics {
		if *t2.TopicArn == *topicARN {
			found = true
			break
		}
	}
	assert.True(t, found, "created topic should appear in ListTopics")

	// GetTopicAttributes
	attrOut, err := snsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: topicARN,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, attrOut.Attributes)

	// DeleteTopic
	_, err = snsClient.DeleteTopic(ctx, &sns.DeleteTopicInput{
		TopicArn: topicARN,
	})
	require.NoError(t, err)

	// Verify gone
	listOut2, err := snsClient.ListTopics(ctx, &sns.ListTopicsInput{})
	require.NoError(t, err)
	for _, t2 := range listOut2.Topics {
		assert.NotEqual(t, *topicARN, *t2.TopicArn, "deleted topic should not appear in ListTopics")
	}
}

func TestIntegration_SNS_SubscribeUnsubscribe(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	// Create a topic
	topicName := "test-sub-topic-" + uuid.NewString()
	topicOut, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	topicARN := topicOut.TopicArn

	// Create an SQS queue
	queueName := "test-sub-queue-" + uuid.NewString()
	queueOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	queueURL := queueOut.QueueUrl

	// Get the queue ARN
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	// Subscribe SQS to SNS topic
	subOut, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topicARN,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
	})
	require.NoError(t, err)
	require.NotNil(t, subOut.SubscriptionArn)
	assert.NotEmpty(t, *subOut.SubscriptionArn)

	subARN := subOut.SubscriptionArn

	// Unsubscribe
	_, err = snsClient.Unsubscribe(ctx, &sns.UnsubscribeInput{
		SubscriptionArn: subARN,
	})
	require.NoError(t, err)
}

func TestIntegration_SNS_PublishToSQS(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	// Create topic
	topicName := "test-pub-topic-" + uuid.NewString()
	topicOut, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	topicARN := topicOut.TopicArn

	// Create SQS queue
	queueName := "test-pub-queue-" + uuid.NewString()
	queueOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	queueURL := queueOut.QueueUrl

	// Get queue ARN
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	// Subscribe SQS to SNS
	_, err = snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topicARN,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
	})
	require.NoError(t, err)

	// Publish a message
	msgText := "sns-to-sqs-" + uuid.NewString()
	pubOut, err := snsClient.Publish(ctx, &sns.PublishInput{
		TopicArn: topicARN,
		Message:  aws.String(msgText),
	})
	require.NoError(t, err)
	require.NotEmpty(t, *pubOut.MessageId)

	// Receive from SQS — SNS should have delivered
	recvOut, err := sqsClient.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            queueURL,
		MaxNumberOfMessages: 1,
		WaitTimeSeconds:     2,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1, "SNS should have delivered message to SQS")
	assert.Contains(t, *recvOut.Messages[0].Body, msgText)
}

func TestIntegration_SNS_ListSubscriptions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	sqsClient := createSQSClient(t)
	ctx := t.Context()

	// Create topic and queue
	topicName := "test-listsub-" + uuid.NewString()
	topicOut, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	topicARN := topicOut.TopicArn

	queueName := "test-listsub-q-" + uuid.NewString()
	queueOut, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	queueURL := queueOut.QueueUrl

	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl:       queueURL,
		AttributeNames: []sqstypes.QueueAttributeName{"QueueArn"},
	})
	require.NoError(t, err)
	queueARN := attrOut.Attributes["QueueArn"]

	// Subscribe
	subOut, err := snsClient.Subscribe(ctx, &sns.SubscribeInput{
		TopicArn: topicARN,
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueARN),
	})
	require.NoError(t, err)
	subARN := *subOut.SubscriptionArn

	// ListSubscriptions
	listSubOut, err := snsClient.ListSubscriptions(ctx, &sns.ListSubscriptionsInput{})
	require.NoError(t, err)
	foundInList := false
	for _, s := range listSubOut.Subscriptions {
		if *s.SubscriptionArn == subARN {
			foundInList = true
			break
		}
	}
	assert.True(t, foundInList, "subscription should appear in ListSubscriptions")

	// ListSubscriptionsByTopic
	listByTopicOut, err := snsClient.ListSubscriptionsByTopic(ctx, &sns.ListSubscriptionsByTopicInput{
		TopicArn: topicARN,
	})
	require.NoError(t, err)
	foundByTopic := false
	for _, s := range listByTopicOut.Subscriptions {
		if *s.SubscriptionArn == subARN {
			foundByTopic = true
			break
		}
	}
	assert.True(t, foundByTopic, "subscription should appear in ListSubscriptionsByTopic")
}

func TestIntegration_SNS_SetTopicAttributes(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	snsClient := createSNSClient(t)
	ctx := t.Context()

	topicName := "test-setattr-" + uuid.NewString()
	createOut, err := snsClient.CreateTopic(ctx, &sns.CreateTopicInput{
		Name: aws.String(topicName),
	})
	require.NoError(t, err)
	topicARN := createOut.TopicArn

	// SetTopicAttributes — update DisplayName
	displayName := "Test Display " + uuid.NewString()
	_, err = snsClient.SetTopicAttributes(ctx, &sns.SetTopicAttributesInput{
		TopicArn:       topicARN,
		AttributeName:  aws.String("DisplayName"),
		AttributeValue: aws.String(displayName),
	})
	require.NoError(t, err)

	// Verify via GetTopicAttributes
	attrOut, err := snsClient.GetTopicAttributes(ctx, &sns.GetTopicAttributesInput{
		TopicArn: topicARN,
	})
	require.NoError(t, err)
	assert.Equal(t, displayName, attrOut.Attributes["DisplayName"])
}
