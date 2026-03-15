package pipes_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// --- mock implementations ---

type mockPipeSQSReader struct {
	mu           sync.Mutex
	messages     []*pipes.PipeSQSMessage
	receiveErr   error
	deleteErr    error
	receiveCalls int
	deletedIDs   []string
}

func (m *mockPipeSQSReader) ReceivePipeMessages(_ string, _ int) ([]*pipes.PipeSQSMessage, error) {
	m.mu.Lock()
	m.receiveCalls++
	msgs := m.messages
	m.messages = nil // clear after read
	m.mu.Unlock()

	if m.receiveErr != nil {
		return nil, m.receiveErr
	}

	return msgs, nil
}

func (m *mockPipeSQSReader) DeletePipeMessages(_ string, receiptHandles []string) error {
	m.mu.Lock()
	m.deletedIDs = append(m.deletedIDs, receiptHandles...)
	m.mu.Unlock()

	return m.deleteErr
}

type mockPipeLambdaInvoker struct {
	mu       sync.Mutex
	calls    []string // function names
	payloads [][]byte
	err      error
}

func (m *mockPipeLambdaInvoker) InvokeFunction(_ context.Context, name, _ string, payload []byte) ([]byte, int, error) {
	m.mu.Lock()
	m.calls = append(m.calls, name)
	m.payloads = append(m.payloads, payload)
	m.mu.Unlock()

	return nil, 200, m.err
}

// --- helper ---

func newTestPipeBackend(t *testing.T) *pipes.InMemoryBackend {
	t.Helper()

	return pipes.NewInMemoryBackend("000000000000", "us-east-1")
}

func createTestPipe(t *testing.T, backend *pipes.InMemoryBackend, name, source, target, state string) {
	t.Helper()

	_, err := backend.CreatePipe(name, "arn:aws:iam::000000000000:role/r", source, target, "", state, nil)
	require.NoError(t, err)
}

// --- tests ---

// TestPipesRunner_SQSToLambda tests that SQS messages are forwarded to a Lambda target.
func TestPipesRunner_SQSToLambda(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
	sqsARN := "arn:aws:sqs:us-east-1:000000000000:my-queue"
	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	createTestPipe(t, backend, "test-pipe", sqsARN, lambdaARN, "RUNNING")

	sqsReader := &mockPipeSQSReader{
		messages: []*pipes.PipeSQSMessage{
			{MessageID: "msg-1", ReceiptHandle: "rh-1", Body: "hello"},
			{MessageID: "msg-2", ReceiptHandle: "rh-2", Body: "world"},
		},
	}
	lambdaInvoker := &mockPipeLambdaInvoker{}

	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)
	runner.SetLambdaInvoker(lambdaInvoker)

	pipes.PollAllPipesOnce(runner, t.Context())

	lambdaInvoker.mu.Lock()
	calls := lambdaInvoker.calls
	payloads := lambdaInvoker.payloads
	lambdaInvoker.mu.Unlock()

	require.Len(t, calls, 1, "expected one Lambda invocation")
	assert.Equal(t, "my-fn", calls[0])

	var event struct {
		Records []struct {
			MessageID   string `json:"messageId"`
			Body        string `json:"body"`
			EventSource string `json:"eventSource"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(payloads[0], &event))
	require.Len(t, event.Records, 2)
	assert.Equal(t, "msg-1", event.Records[0].MessageID)
	assert.Equal(t, "hello", event.Records[0].Body)
	assert.Equal(t, "aws:sqs", event.Records[0].EventSource)

	// Messages should be deleted after successful invocation
	sqsReader.mu.Lock()
	deleted := sqsReader.deletedIDs
	sqsReader.mu.Unlock()

	assert.ElementsMatch(t, []string{"rh-1", "rh-2"}, deleted)
}

// TestPipesRunner_StoppedPipeSkipped tests that STOPPED pipes are not polled.
func TestPipesRunner_StoppedPipeSkipped(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
	createTestPipe(t, backend, "stopped-pipe",
		"arn:aws:sqs:us-east-1:000000000000:q",
		"arn:aws:lambda:us-east-1:000000000000:function:fn",
		"STOPPED")

	sqsReader := &mockPipeSQSReader{
		messages: []*pipes.PipeSQSMessage{{MessageID: "m1", Body: "test"}},
	}
	lambdaInvoker := &mockPipeLambdaInvoker{}

	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)
	runner.SetLambdaInvoker(lambdaInvoker)

	pipes.PollAllPipesOnce(runner, t.Context())

	lambdaInvoker.mu.Lock()
	calls := lambdaInvoker.calls
	lambdaInvoker.mu.Unlock()

	assert.Empty(t, calls, "STOPPED pipe should not trigger Lambda")

	sqsReader.mu.Lock()
	receiveCalls := sqsReader.receiveCalls
	sqsReader.mu.Unlock()

	assert.Equal(t, 0, receiveCalls, "SQS should not be polled for STOPPED pipe")
}

// TestPipesRunner_SQSReceiveError tests graceful handling of SQS receive errors.
func TestPipesRunner_SQSReceiveError(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
	createTestPipe(t, backend, "err-pipe",
		"arn:aws:sqs:us-east-1:000000000000:q",
		"arn:aws:lambda:us-east-1:000000000000:function:fn",
		"RUNNING")

	sqsReader := &mockPipeSQSReader{receiveErr: assert.AnError}
	lambdaInvoker := &mockPipeLambdaInvoker{}

	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)
	runner.SetLambdaInvoker(lambdaInvoker)

	// Should not panic
	pipes.PollAllPipesOnce(runner, t.Context())

	lambdaInvoker.mu.Lock()
	calls := lambdaInvoker.calls
	lambdaInvoker.mu.Unlock()

	assert.Empty(t, calls, "Lambda should not be invoked when SQS receive fails")
}

// TestPipesRunner_EmptyQueueSkipsInvocation tests that no Lambda invocation occurs for an empty queue.
func TestPipesRunner_EmptyQueueSkipsInvocation(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
	createTestPipe(t, backend, "empty-pipe",
		"arn:aws:sqs:us-east-1:000000000000:q",
		"arn:aws:lambda:us-east-1:000000000000:function:fn",
		"RUNNING")

	sqsReader := &mockPipeSQSReader{messages: nil}
	lambdaInvoker := &mockPipeLambdaInvoker{}

	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)
	runner.SetLambdaInvoker(lambdaInvoker)

	pipes.PollAllPipesOnce(runner, t.Context())

	lambdaInvoker.mu.Lock()
	calls := lambdaInvoker.calls
	lambdaInvoker.mu.Unlock()

	assert.Empty(t, calls, "Lambda should not be invoked when queue is empty")
}

// TestPipesRunner_StartAndShutdown tests the runner goroutine lifecycle.
func TestPipesRunner_StartAndShutdown(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
	runner := pipes.NewRunner(backend)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	runner.Start(ctx)
	<-ctx.Done()
	// No panic - goroutine should have stopped cleanly
}

// TestPipesHandler_StartWorkerAndShutdown tests that the handler implements BackgroundWorker.
func TestPipesHandler_StartWorkerAndShutdown(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	require.NoError(t, h.StartWorker(ctx))
	<-ctx.Done()
	h.Shutdown(t.Context())
}
