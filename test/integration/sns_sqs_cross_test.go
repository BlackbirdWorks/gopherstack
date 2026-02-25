package integration_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
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
