package integration_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	pipessdk "github.com/aws/aws-sdk-go-v2/service/pipes"
	sqssdk "github.com/aws/aws-sdk-go-v2/service/sqs"
	sqssdktypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/google/uuid"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"log/slog"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	pipespkg "github.com/blackbirdworks/gopherstack/services/pipes"
	sqspkg "github.com/blackbirdworks/gopherstack/services/sqs"
)

const (
	// pipesPollTimeout is the max time to wait for pipe messages to be forwarded.
	pipesPollTimeout = 5 * time.Second
	// pipesPollInterval is how often to check for Lambda invocations.
	pipesPollInterval = 50 * time.Millisecond
)

// mockPipesLambdaInvoker records Lambda invocations from the Pipes runner.
type mockPipesLambdaInvoker struct {
	calls    []string
	payloads [][]byte
	mu       sync.Mutex
}

func (m *mockPipesLambdaInvoker) InvokeFunction(
	_ context.Context,
	name, _ string,
	payload []byte,
) ([]byte, int, error) {
	m.mu.Lock()
	m.calls = append(m.calls, name)
	m.payloads = append(m.payloads, payload)
	m.mu.Unlock()

	return nil, 200, nil
}

func (m *mockPipesLambdaInvoker) CallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.calls)
}

// testPipesSQSReaderAdapter adapts the SQS InMemoryBackend to the pipes.SQSReader interface.
type testPipesSQSReaderAdapter struct {
	backend *sqspkg.InMemoryBackend
}

func (a *testPipesSQSReaderAdapter) ReceivePipeMessages(
	queueARN string,
	maxMessages int,
) ([]*pipespkg.SQSMessage, error) {
	url := pipesARNToURL(queueARN)

	msgs, err := a.backend.ReceiveMessagesLocal(url, maxMessages)
	if err != nil {
		return nil, err
	}

	result := make([]*pipespkg.SQSMessage, len(msgs))
	for i, m := range msgs {
		result[i] = &pipespkg.SQSMessage{
			MessageID:     m.MessageID,
			ReceiptHandle: m.ReceiptHandle,
			Body:          m.Body,
			Attributes:    m.Attributes,
			MD5OfBody:     m.MD5OfBody,
		}
	}

	return result, nil
}

func (a *testPipesSQSReaderAdapter) DeletePipeMessages(queueARN string, receiptHandles []string) error {
	url := pipesARNToURL(queueARN)

	return a.backend.DeleteMessagesLocal(url, receiptHandles)
}

// pipesARNToURL converts an SQS ARN to a local queue URL.
func pipesARNToURL(arn string) string {
	parts := strings.Split(arn, ":")

	const minParts = 6
	if len(parts) < minParts {
		return arn
	}

	return "http://local/" + parts[4] + "/" + parts[5]
}

// TestIntegration_Pipes_SQS_To_Lambda verifies that messages placed on an SQS queue are
// forwarded by the Pipes Runner to a Lambda target.
//
// This test uses in-process backends and a mock Lambda invoker (no Docker required).
func TestIntegration_Pipes_SQS_To_Lambda(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ctx := t.Context()

	// --- Build backends ---
	sqsBackend := sqspkg.NewInMemoryBackendWithConfig("000000000000", "us-east-1")
	sqsHandler := sqspkg.NewHandler(sqsBackend)
	sqsHandler.DefaultRegion = "us-east-1"

	pipesBackend := pipespkg.NewInMemoryBackend("000000000000", "us-east-1")
	pipesHandler := pipespkg.NewHandler(pipesBackend)

	// Wire mock Lambda invoker and SQS reader into the pipes runner.
	mockInvoker := &mockPipesLambdaInvoker{}
	pipesHandler.GetRunner().SetLambdaInvoker(mockInvoker)
	pipesHandler.GetRunner().SetSQSReader(&testPipesSQSReaderAdapter{backend: sqsBackend})

	// --- Set up in-process HTTP server ---
	e := echo.New()
	e.Pre(logger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(sqsHandler))
	require.NoError(t, registry.Register(pipesHandler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	// --- Build AWS SDK clients ---
	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	sqsClient := sqssdk.NewFromConfig(awsCfg, func(o *sqssdk.Options) { o.BaseEndpoint = aws.String(server.URL) })
	pipesClient := pipessdk.NewFromConfig(awsCfg, func(o *pipessdk.Options) { o.BaseEndpoint = aws.String(server.URL) })

	// --- Step 1: Create SQS queue ---
	queueName := "pipes-test-" + uuid.NewString()

	createOut, err := sqsClient.CreateQueue(ctx, &sqssdk.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	require.NoError(t, err)

	queueURL := aws.ToString(createOut.QueueUrl)

	// Get queue ARN.
	attrOut, err := sqsClient.GetQueueAttributes(ctx, &sqssdk.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL),
		AttributeNames: []sqssdktypes.QueueAttributeName{sqssdktypes.QueueAttributeNameQueueArn},
	})
	require.NoError(t, err)

	queueARN := attrOut.Attributes[string(sqssdktypes.QueueAttributeNameQueueArn)]
	require.NotEmpty(t, queueARN)

	// --- Step 2: Create a Pipe (SQS → Lambda) ---
	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:pipe-target-fn"

	_, err = pipesClient.CreatePipe(ctx, &pipessdk.CreatePipeInput{
		Name:    aws.String("test-pipe-" + uuid.NewString()),
		RoleArn: aws.String("arn:aws:iam::000000000000:role/r"),
		Source:  aws.String(queueARN),
		Target:  aws.String(lambdaARN),
	})
	require.NoError(t, err)

	// --- Step 3: Start the Pipes runner ---
	runCtx, runCancel := context.WithCancel(ctx)
	t.Cleanup(runCancel)
	pipesHandler.GetRunner().Start(runCtx)

	// --- Step 4: Send messages to the SQS queue ---
	_, err = sqsClient.SendMessage(ctx, &sqssdk.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String("hello from pipes"),
	})
	require.NoError(t, err)

	_, err = sqsClient.SendMessage(ctx, &sqssdk.SendMessageInput{
		QueueUrl:    aws.String(queueURL),
		MessageBody: aws.String("world from pipes"),
	})
	require.NoError(t, err)

	// --- Step 5: Wait for Lambda to be invoked ---
	deadline := time.Now().Add(pipesPollTimeout)

	for time.Now().Before(deadline) {
		if mockInvoker.CallCount() >= 1 {
			break
		}

		time.Sleep(pipesPollInterval)
	}

	require.GreaterOrEqual(t, mockInvoker.CallCount(), 1, "Lambda should be invoked by the Pipes runner")

	// --- Step 6: Verify the event payload ---
	mockInvoker.mu.Lock()
	payload := mockInvoker.payloads[0]
	mockInvoker.mu.Unlock()

	var event struct {
		Records []struct {
			Body        string `json:"body"`
			EventSource string `json:"eventSource"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(payload, &event))

	assert.NotEmpty(t, event.Records, "payload should have at least one record")
	assert.Equal(t, "aws:sqs", event.Records[0].EventSource)
}
