package sns_test

import (
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	snssdk "github.com/aws/aws-sdk-go-v2/service/sns"
	snstypes "github.com/aws/aws-sdk-go-v2/service/sns/types"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/events"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sns"
	"github.com/blackbirdworks/gopherstack/services/sqs"
)

// newTestSQSClient stands up the real aws-sdk-go-v2 SQS client against an
// httptest server running backend's Handler, mirroring newTestSNSClient.
func newTestSQSClient(t *testing.T, backend *sqs.InMemoryBackend) *sqssdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(sqs.NewHandler(backend)))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err)

	return sqssdk.NewFromConfig(cfg, func(o *sqssdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
}

// TestRealClient_FilterPolicyCIDR_SQSDelivery drives the "cidr" FilterPolicy
// operator end-to-end through the real aws-sdk-go-v2 SNS and SQS clients,
// wired the same way production does (SNS publish emitter -> SQS
// SubscribeToSNS). An SQS queue subscribed with a source_ip cidr filter must
// receive only the publish whose source_ip attribute falls inside the block.
func TestRealClient_FilterPolicyCIDR_SQSDelivery(t *testing.T) {
	t.Parallel()

	snsBackend := sns.NewInMemoryBackend()
	sqsBackend := sqs.NewInMemoryBackend()
	t.Cleanup(sqsBackend.Close)

	emitter := events.NewInMemoryEmitter[*events.SNSPublishedEvent]()
	snsBackend.SetPublishEmitter(emitter)
	sqsBackend.SubscribeToSNS(emitter)

	snsClient := newTestSNSClient(t, sns.NewHandler(snsBackend))
	sqsClient := newTestSQSClient(t, sqsBackend)
	ctx := t.Context()

	topicOut, err := snsClient.CreateTopic(ctx, &snssdk.CreateTopicInput{
		Name: aws.String("cidr-filter-topic"),
	})
	require.NoError(t, err)
	topicArn := aws.ToString(topicOut.TopicArn)

	queueOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{
		QueueName: aws.String("cidr-filter-queue"),
	})
	require.NoError(t, err)
	queueURL := aws.ToString(queueOut.QueueUrl)

	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqstypes.QueueAttributeName{sqstypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)
	queueArn := attrOut.Attributes["QueueArn"]

	_, err = snsClient.Subscribe(ctx, &snssdk.SubscribeInput{
		TopicArn: aws.String(topicArn),
		Protocol: aws.String("sqs"),
		Endpoint: aws.String(queueArn),
		Attributes: map[string]string{
			"FilterPolicy":       `{"source_ip":[{"cidr":"10.0.0.0/24"}]}`,
			"RawMessageDelivery": "true",
		},
	})
	require.NoError(t, err)

	publish := func(sourceIP string) {
		t.Helper()

		_, pubErr := snsClient.Publish(ctx, &snssdk.PublishInput{
			TopicArn: aws.String(topicArn),
			Message:  aws.String("from-" + sourceIP),
			MessageAttributes: map[string]snstypes.MessageAttributeValue{
				"source_ip": {
					DataType:    aws.String("String"),
					StringValue: aws.String(sourceIP),
				},
			},
		})
		require.NoError(t, pubErr)
	}

	publish("172.16.0.5") // outside the /24 block; must be filtered out
	publish("10.0.0.42")  // inside the /24 block; must be delivered

	recvOut, err := sqsClient.ReceiveMessage(ctx, &sqssdk.ReceiveMessageInput{
		QueueUrl:            aws.String(queueURL),
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1, "only the in-CIDR publish should reach the queue")
	assert.Equal(t, "from-10.0.0.42", aws.ToString(recvOut.Messages[0].Body))
}
