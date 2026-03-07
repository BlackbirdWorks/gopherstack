package integration_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdaesdktypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqssdktypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	lambdapkg "github.com/blackbirdworks/gopherstack/services/lambda"
	sqspkg "github.com/blackbirdworks/gopherstack/services/sqs"
)

const (
	// esmTestPortStart is the start of the port range reserved for ESM integration tests.
	esmTestPortStart = 19020
	// esmTestPortEnd is the exclusive end of the ESM test port range.
	esmTestPortEnd = 19030
	// esmInvokeTimeout is the per-invocation deadline for the Lambda container cold-start.
	esmInvokeTimeout = 30 * time.Second
	// esmDrainTimeout is the maximum time to wait for the SQS queue to drain to zero messages.
	esmDrainTimeout = 60 * time.Second
	// esmDrainPollInterval is how often to check the queue during the drain wait.
	esmDrainPollInterval = 500 * time.Millisecond
	// esmMessageCount is the number of SQS messages sent in the drain test.
	esmMessageCount = 5
)

// noopKinesisReader is a no-op KinesisReader used when the ESM poller only needs to
// handle SQS sources in the test.
type noopKinesisReader struct{}

func (r *noopKinesisReader) GetShardIDs(_ string) ([]string, error) { return nil, nil }

func (r *noopKinesisReader) GetShardIterator(_, _, _, _ string) (string, error) { return "", nil }

func (r *noopKinesisReader) GetRecords(_ string, _ int) ([]lambdapkg.KinesisRecord, string, error) {
	return nil, "", nil
}

// testSQSReaderAdapter adapts the SQS InMemoryBackend to the lambda.SQSReader interface.
type testSQSReaderAdapter struct {
	backend *sqspkg.InMemoryBackend
}

func (a *testSQSReaderAdapter) ReceiveMessagesLocal(queueARN string, maxMessages int) ([]*lambdapkg.SQSMessage, error) {
	url := sqsARNToURL(queueARN)

	msgs, err := a.backend.ReceiveMessagesLocal(url, maxMessages)
	if err != nil {
		return nil, err
	}

	result := make([]*lambdapkg.SQSMessage, len(msgs))
	for i, m := range msgs {
		result[i] = &lambdapkg.SQSMessage{
			MessageID:     m.MessageID,
			ReceiptHandle: m.ReceiptHandle,
			Body:          m.Body,
			Attributes:    m.Attributes,
			MD5OfBody:     m.MD5OfBody,
		}
	}

	return result, nil
}

func (a *testSQSReaderAdapter) DeleteMessagesLocal(queueARN string, receiptHandles []string) error {
	url := sqsARNToURL(queueARN)

	return a.backend.DeleteMessagesLocal(url, receiptHandles)
}

// sqsARNToURL converts an SQS ARN (arn:aws:sqs:region:account:name) to a URL that the
// SQS InMemoryBackend can resolve via queueNameFromInput.
func sqsARNToURL(arn string) string {
	parts := strings.Split(arn, ":")

	const minParts = 6
	if len(parts) < minParts {
		return arn
	}

	accountID := parts[4]
	queueName := parts[5]

	return "http://local/" + accountID + "/" + queueName
}

// TestIntegration_Lambda_SQS_ESM verifies that SQS messages are consumed by the ESM poller
// and delivered to a Lambda function, causing the queue to drain to zero messages.
//
// Test flow:
//  1. Build a minimal Go echo Lambda image (reusing buildEchoLambdaImage).
//  2. Start an in-process Gopherstack server with Lambda and SQS handlers wired together.
//  3. CreateQueue → GetQueueAttributes (to get the queue ARN) → CreateFunction → CreateESM.
//  4. Send 5 SQS messages.
//  5. Start the ESM poller.
//  6. Wait (up to 60 s) for ApproximateNumberOfMessages and ApproximateNumberOfMessagesNotVisible to both reach 0.
//  7. Verify that the queue is empty.
func TestIntegration_Lambda_SQS_ESM(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	// Attempt to create a container runtime. Skip gracefully if Docker is unavailable.
	dc, err := container.NewRuntime(container.Config{
		Logger:      slog.Default(),
		PoolSize:    lambdaContainerPoolSize,
		IdleTimeout: lambdaContainerIdleTimeout,
	})
	if err != nil {
		t.Skipf("container runtime unavailable for Lambda ESM integration test: %v", err)
	}

	t.Cleanup(func() { _ = dc.Close() })

	// Build (or rebuild) the echo Lambda image before starting the service.
	buildEchoLambdaImage(ctx, t)

	// Create a port allocator for Lambda Runtime API servers.
	alloc, err := portalloc.New(esmTestPortStart, esmTestPortEnd)
	require.NoError(t, err)

	// --- Build service backends ---
	sqsBackend := sqspkg.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	sqsHandler := sqspkg.NewHandler(sqsBackend)
	sqsHandler.DefaultRegion = "us-east-1"

	lambdaSettings := lambdapkg.DefaultSettings()
	lambdaBackend := lambdapkg.NewInMemoryBackend(dc, alloc, lambdaSettings, "000000000000", "us-east-1")
	lambdaHandler := lambdapkg.NewHandler(lambdaBackend)
	lambdaHandler.AccountID = "000000000000"
	lambdaHandler.DefaultRegion = "us-east-1"

	// Wire SQS → Lambda ESM poller.
	poller := lambdapkg.NewEventSourcePoller(lambdaBackend, &noopKinesisReader{})
	lambdaBackend.SetKinesisPoller(poller)
	poller.SetSQSReader(&testSQSReaderAdapter{backend: sqsBackend})

	// Start in-process HTTP server with both handlers registered.
	e := echo.New()
	e.Pre(logger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(lambdaHandler))
	require.NoError(t, registry.Register(sqsHandler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	// Build AWS SDK clients pointing at the in-process server.
	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	sqsClient := sqssdk.NewFromConfig(awsCfg, func(o *sqssdk.Options) {
		o.BaseEndpoint = aws.String(server.URL)
	})
	lambdaClient := lambdasdk.NewFromConfig(awsCfg, func(o *lambdasdk.Options) {
		o.BaseEndpoint = aws.String(server.URL)
	})

	// --- Step 1: CreateQueue ---
	queueName := "esm-drain-test-" + uuid.NewString()

	createQueueOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)
	queueURL := aws.ToString(createQueueOut.QueueUrl)

	// --- Step 2: Get queue ARN from attributes ---
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqssdktypes.QueueAttributeName{sqssdktypes.QueueAttributeNameAll},
	})
	require.NoError(t, err)

	queueARN, ok := attrOut.Attributes["QueueArn"]
	require.True(t, ok, "QueueArn attribute should be present")
	require.NotEmpty(t, queueARN)

	// --- Step 3: CreateFunction (echo handler) ---
	funcName := "esm-test-" + uuid.NewString()

	createFnBody, err := json.Marshal(map[string]any{
		"FunctionName": funcName,
		"PackageType":  "Image",
		"Code":         map[string]string{"ImageUri": lambdaEchoImage},
		"Role":         "arn:aws:iam::000000000000:role/lambda-test",
		"Timeout":      int(esmInvokeTimeout.Seconds()),
	})
	require.NoError(t, err)

	createFnResp, err := doLambdaRequest(ctx, "POST",
		server.URL+"/2015-03-31/functions", "application/json",
		strings.NewReader(string(createFnBody)))
	require.NoError(t, err)

	defer createFnResp.Body.Close()

	require.Equal(t, 201, createFnResp.StatusCode, "CreateFunction should return 201")

	// --- Step 4: CreateEventSourceMapping (SQS queue → Lambda function) ---
	createESMOut, err := lambdaClient.CreateEventSourceMapping(ctx, &lambdasdk.CreateEventSourceMappingInput{
		EventSourceArn:   aws.String(queueARN),
		FunctionName:     aws.String(funcName),
		BatchSize:        aws.Int32(10),
		StartingPosition: lambdaesdktypes.EventSourcePositionTrimHorizon,
	})
	require.NoError(t, err)
	esmUUID := aws.ToString(createESMOut.UUID)
	require.NotEmpty(t, esmUUID)

	// --- Step 5: Start the ESM poller ---
	esmCtx, esmCancel := context.WithCancel(ctx)
	t.Cleanup(esmCancel)

	poller.Start(esmCtx)

	// --- Step 6: Send 5 messages to the SQS queue ---
	for i := range esmMessageCount {
		_, sendErr := sqsClient.SendMessage(ctx, &sqssdk.SendMessageInput{
			QueueUrl:    aws.String(queueURL),
			MessageBody: aws.String("esm-test-message-" + strconv.Itoa(i)),
		})
		require.NoError(t, sendErr)
	}

	// --- Step 7: Wait for queue to drain ---
	// The ESM poller invokes Lambda (container cold start) and deletes messages on success.
	assert.Eventually(t, func() bool {
		drainAttr, drainErr := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
			QueueUrl: aws.String(queueURL),
			AttributeNames: []sqssdktypes.QueueAttributeName{
				sqssdktypes.QueueAttributeNameApproximateNumberOfMessages,
				sqssdktypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
			},
		})
		if drainErr != nil {
			return false
		}

		visible := drainAttr.Attributes[string(sqssdktypes.QueueAttributeNameApproximateNumberOfMessages)]
		inFlight := drainAttr.Attributes[string(sqssdktypes.QueueAttributeNameApproximateNumberOfMessagesNotVisible)]

		return visible == "0" && inFlight == "0"
	}, esmDrainTimeout, esmDrainPollInterval, "SQS queue should drain to 0 messages after Lambda ESM processing")

	// --- Cleanup ---
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, _ = lambdaClient.DeleteEventSourceMapping(cleanupCtx, &lambdasdk.DeleteEventSourceMappingInput{
			UUID: aws.String(esmUUID),
		})

		_, _ = lambdaClient.DeleteFunction(cleanupCtx, &lambdasdk.DeleteFunctionInput{
			FunctionName: aws.String(funcName),
		})

		_, _ = sqsClient.DeleteQueue(cleanupCtx, &sqssdk.DeleteQueueInput{
			QueueUrl: aws.String(queueURL),
		})
	})
}
