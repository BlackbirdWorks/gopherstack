package main

import (
	"context"
	"encoding/json"
	"io"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	s3sdk "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
	lambdabackend "github.com/blackbirdworks/gopherstack/services/lambda"
	snsbackend "github.com/blackbirdworks/gopherstack/services/sns"
	sqsbackend "github.com/blackbirdworks/gopherstack/services/sqs"
)

// wiringMockS3Storer implements firehose.S3Storer so the test can observe that a
// record delivered through the SNS→Firehose path actually reached S3.
type wiringMockS3Storer struct {
	calls []wiringS3PutCall
}

type wiringS3PutCall struct {
	bucket string
	key    string
	body   []byte
}

func (m *wiringMockS3Storer) PutObject(
	_ context.Context,
	input *s3sdk.PutObjectInput,
) (*s3sdk.PutObjectOutput, error) {
	body, _ := io.ReadAll(input.Body)
	m.calls = append(m.calls, wiringS3PutCall{
		bucket: aws.ToString(input.Bucket),
		key:    aws.ToString(input.Key),
		body:   body,
	})

	return &s3sdk.PutObjectOutput{}, nil
}

// TestWireSNSToLambdaFirehose_EndToEndDelivery exercises the exact composition-root
// wiring used by cli.go's setup (wireSNSToLambdaFirehose) against REAL Lambda, Firehose,
// and SQS backends — not test doubles — to prove that subscribing a Lambda function or
// Firehose delivery stream to an SNS topic and publishing actually delivers, instead of
// silently no-opping as it did before SetLambdaBackend/SetFirehoseBackend/SetSQSSender
// were wired in the running binary.
func TestWireSNSToLambdaFirehose_EndToEndDelivery(t *testing.T) {
	t.Parallel()

	snsBk := snsbackend.NewInMemoryBackend()
	snsH := snsbackend.NewHandler(snsBk)

	lambdaBk := lambdabackend.NewInMemoryBackend(
		nil, nil, lambdabackend.DefaultSettings(), config.DefaultAccountID, config.DefaultRegion,
	)
	lambdaH := lambdabackend.NewHandler(lambdaBk)

	firehoseBk := firehosebackend.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	firehoseH := firehosebackend.NewHandler(firehoseBk)

	sqsBk := sqsbackend.NewInMemoryBackend()
	sqsH := sqsbackend.NewHandler(sqsBk)

	// This is the exact call cli.go's setupServices makes.
	wireSNSToLambdaFirehose(snsH, lambdaH, firehoseH, sqsH)

	ctx := context.Background()

	// --- SNS -> Firehose: publish must reach the real Firehose backend and flush to S3. ---
	s3mock := &wiringMockS3Storer{}
	firehoseBk.SetS3Backend(s3mock)

	_, err := firehoseBk.CreateDeliveryStream(ctx, firehosebackend.CreateDeliveryStreamInput{
		Name: "wiring-test-stream",
		S3Destination: &firehosebackend.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::wiring-test-bucket",
			BufferingHints: &firehosebackend.BufferingHints{
				SizeInMBs:         128,
				IntervalInSeconds: 900,
			},
		},
	})
	require.NoError(t, err)

	firehoseTopic, err := snsBk.CreateTopic("firehose-topic", nil)
	require.NoError(t, err)

	firehoseStreamARN := arn.Build(
		"firehose", config.DefaultRegion, config.DefaultAccountID, "deliverystream/wiring-test-stream",
	)
	_, err = snsBk.Subscribe(firehoseTopic.TopicArn, "firehose", firehoseStreamARN, "")
	require.NoError(t, err)

	_, err = snsBk.Publish(firehoseTopic.TopicArn, "hello-firehose", "", "", nil)
	require.NoError(t, err)

	firehoseBk.FlushAll(ctx)

	require.Len(t, s3mock.calls, 1,
		"SNS publish must have been delivered to the real Firehose backend via cli.go's wireSNSToLambdaFirehose")
	assert.Contains(t, string(s3mock.calls[0].body), "hello-firehose")

	// --- SNS -> Lambda with DLQ: publish must reach the real Lambda backend. There is
	// no Docker runtime available in this test, so the invocation fails deterministically
	// (a real failure mode, not a stub) — proving delivery requires the failed message to
	// be redirected to the real SQS DLQ via the wired SQS sender. ---
	dlqOut, err := sqsBk.CreateQueue(&sqsbackend.CreateQueueInput{QueueName: "lambda-dlq"})
	require.NoError(t, err)

	lambdaTopic, err := snsBk.CreateTopic("lambda-topic", nil)
	require.NoError(t, err)

	functionARN := arn.Build(
		"lambda", config.DefaultRegion, config.DefaultAccountID, "function:wiring-test-fn",
	)
	sub, err := snsBk.Subscribe(lambdaTopic.TopicArn, "lambda", functionARN, "")
	require.NoError(t, err)

	dlqARN := arn.Build("sqs", config.DefaultRegion, config.DefaultAccountID, "lambda-dlq")
	redrivePolicy, err := json.Marshal(map[string]string{"deadLetterTargetArn": dlqARN})
	require.NoError(t, err)
	require.NoError(t, snsBk.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy)))

	_, err = snsBk.Publish(lambdaTopic.TopicArn, "hello-lambda", "", "", nil)
	require.NoError(t, err)

	recvOut, err := sqsBk.ReceiveMessage(&sqsbackend.ReceiveMessageInput{
		QueueURL:            dlqOut.QueueURL,
		MaxNumberOfMessages: 1,
	})
	require.NoError(t, err)
	require.Len(t, recvOut.Messages, 1,
		"failed Lambda delivery must have been redirected to the real SQS DLQ via cli.go's wireSNSToLambdaFirehose "+
			"(proves both SetLambdaBackend and SetSQSSender were wired)")
	assert.Equal(t, "hello-lambda", recvOut.Messages[0].Body)
}
