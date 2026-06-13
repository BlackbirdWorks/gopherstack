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

type mockSQSReader struct {
	receiveErr      error
	deleteErr       error
	messages        []*pipes.SQSMessage
	deletedIDs      []string
<<<<<<< Updated upstream
=======
	deleted         []string // alias for deletedIDs used in filter tests
>>>>>>> Stashed changes
	receiveCalls    int
	lastMaxMessages int
	mu              sync.Mutex
}

func (m *mockSQSReader) ReceivePipeMessages(_ string, maxMessages int) ([]*pipes.SQSMessage, error) {
	m.mu.Lock()
	m.receiveCalls++
	m.lastMaxMessages = maxMessages
	msgs := m.messages
	m.messages = nil // clear after read
	m.mu.Unlock()

	if m.receiveErr != nil {
		return nil, m.receiveErr
	}

	return msgs, nil
}

func (m *mockSQSReader) DeletePipeMessages(_ string, receiptHandles []string) error {
	m.mu.Lock()
	m.deletedIDs = append(m.deletedIDs, receiptHandles...)
	m.deleted = append(m.deleted, receiptHandles...)
	m.mu.Unlock()

	return m.deleteErr
}

func (m *mockSQSReader) getDeleted() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return append([]string(nil), m.deletedIDs...)
}

func (m *mockSQSReader) getLastMaxMessages() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.lastMaxMessages
}

type mockPipeLambdaInvoker struct {
	err      error
	calls    []string
	payloads [][]byte
	mu       sync.Mutex
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

	_, err := backend.CreatePipeSimple(name, "arn:aws:iam::000000000000:role/r", source, target, "", state, nil)
	require.NoError(t, err)

	if state == "RUNNING" {
		pipes.WaitPipeRunning(t, backend, name)
	}
}

// --- tests ---

// TestPipesRunner_SQSToLambda tests that SQS messages are forwarded to a Lambda target.
func TestPipesRunner_SQSToLambda(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
	sqsARN := "arn:aws:sqs:us-east-1:000000000000:my-queue"
	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	createTestPipe(t, backend, "test-pipe", sqsARN, lambdaARN, "RUNNING")

	sqsReader := &mockSQSReader{
		messages: []*pipes.SQSMessage{
			{MessageID: "msg-1", ReceiptHandle: "rh-1", Body: "hello"},
			{MessageID: "msg-2", ReceiptHandle: "rh-2", Body: "world"},
		},
	}
	lambdaInvoker := &mockPipeLambdaInvoker{}

	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)
	runner.SetLambdaInvoker(lambdaInvoker)

	pipes.PollAllPipesOnce(t.Context(), runner)

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

	sqsReader := &mockSQSReader{
		messages: []*pipes.SQSMessage{{MessageID: "m1", Body: "test"}},
	}
	lambdaInvoker := &mockPipeLambdaInvoker{}

	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)
	runner.SetLambdaInvoker(lambdaInvoker)

	pipes.PollAllPipesOnce(t.Context(), runner)

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

	sqsReader := &mockSQSReader{receiveErr: assert.AnError}
	lambdaInvoker := &mockPipeLambdaInvoker{}

	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)
	runner.SetLambdaInvoker(lambdaInvoker)

	// Should not panic
	pipes.PollAllPipesOnce(t.Context(), runner)

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

	sqsReader := &mockSQSReader{messages: nil}
	lambdaInvoker := &mockPipeLambdaInvoker{}

	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)
	runner.SetLambdaInvoker(lambdaInvoker)

	pipes.PollAllPipesOnce(t.Context(), runner)

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

// TestPipesRunner_FilterCriteria tests that message filtering is applied before target invocation.
func TestPipesRunner_FilterCriteria(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
	sqsARN := "arn:aws:sqs:us-east-1:000000000000:filter-queue"
	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:my-fn"

	_, err := backend.CreatePipe(pipes.CreatePipeInput{
		Name:         "filter-pipe",
		RoleARN:      "arn:aws:iam::000000000000:role/r",
		Source:       sqsARN,
		Target:       lambdaARN,
		DesiredState: "RUNNING",
		SourceParameters: &pipes.SourceParameters{
			FilterCriteria: &pipes.FilterCriteria{
				Filters: []pipes.Filter{{Pattern: "order"}},
			},
		},
	})
	require.NoError(t, err)
<<<<<<< Updated upstream
	pipes.WaitPipeRunning(t, backend, "filter-pipe")
=======
>>>>>>> Stashed changes

	sqsReader := &mockSQSReader{
		messages: []*pipes.SQSMessage{
			{MessageID: "m1", ReceiptHandle: "rh1", Body: `{"type":"order","id":1}`},
			{MessageID: "m2", ReceiptHandle: "rh2", Body: `{"type":"inventory","id":2}`},
		},
	}
	lambdaInvoker := &mockPipeLambdaInvoker{}

	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)
	runner.SetLambdaInvoker(lambdaInvoker)

	pipes.PollAllPipesOnce(t.Context(), runner)

	lambdaInvoker.mu.Lock()
	calls := lambdaInvoker.calls
	payloads := lambdaInvoker.payloads
	lambdaInvoker.mu.Unlock()

	require.Len(t, calls, 1)

<<<<<<< Updated upstream
	var event map[string]any
	require.NoError(t, json.Unmarshal(payloads[0], &event))
	require.Len(t, event, 1)
	// payload is forwarded - just check Lambda was called once

	deleted := sqsReader.getDeleted()
=======
	var event struct {
		Records []struct{ MessageID string `json:"messageId"` } `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(payloads[0], &event))
	require.Len(t, event.Records, 1)
	assert.Equal(t, "m1", event.Records[0].MessageID)

	// Only the matched message should be deleted.
	sqsReader.mu.Lock()
	deleted := sqsReader.deleted
	sqsReader.mu.Unlock()
>>>>>>> Stashed changes
	assert.Equal(t, []string{"rh1"}, deleted)
}

// TestPipesRunner_ConfigurableBatchSize tests that SourceParameters.BatchSize is used.
func TestPipesRunner_ConfigurableBatchSize(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
<<<<<<< Updated upstream
	_, err := backend.CreatePipe(pipes.CreatePipeInput{
		Name:         "batch-pipe",
		RoleARN:      "arn:aws:iam::000000000000:role/r",
		Source:       "arn:aws:sqs:us-east-1:000000000000:batch-queue",
		Target:       "arn:aws:lambda:us-east-1:000000000000:function:fn",
=======
	sqsARN := "arn:aws:sqs:us-east-1:000000000000:batch-queue"
	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:fn"

	_, err := backend.CreatePipe(pipes.CreatePipeInput{
		Name:         "batch-pipe",
		RoleARN:      "arn:aws:iam::000000000000:role/r",
		Source:       sqsARN,
		Target:       lambdaARN,
>>>>>>> Stashed changes
		DesiredState: "RUNNING",
		SourceParameters: &pipes.SourceParameters{
			SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 3},
		},
	})
	require.NoError(t, err)
<<<<<<< Updated upstream
	pipes.WaitPipeRunning(t, backend, "batch-pipe")
=======
>>>>>>> Stashed changes

	sqsReader := &mockSQSReader{}
	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)

	pipes.PollAllPipesOnce(t.Context(), runner)

<<<<<<< Updated upstream
	assert.Equal(t, 3, sqsReader.getLastMaxMessages(), "runner should request batch size from source parameters")
}

// TestPipesRunner_InputTemplate tests that TargetParameters.InputTemplate overrides default payload.
=======
	sqsReader.mu.Lock()
	maxRequested := sqsReader.lastMaxMessages
	sqsReader.mu.Unlock()

	assert.Equal(t, 3, maxRequested, "runner should request batch size from source parameters")
}

// TestPipesRunner_InputTemplate tests that TargetParameters.InputTemplate overrides the default payload.
>>>>>>> Stashed changes
func TestPipesRunner_InputTemplate(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
<<<<<<< Updated upstream
	_, err := backend.CreatePipe(pipes.CreatePipeInput{
		Name:         "template-pipe",
		RoleARN:      "arn:aws:iam::000000000000:role/r",
		Source:       "arn:aws:sqs:us-east-1:000000000000:tmpl-queue",
		Target:       "arn:aws:lambda:us-east-1:000000000000:function:fn",
=======
	sqsARN := "arn:aws:sqs:us-east-1:000000000000:tmpl-queue"
	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:fn"

	_, err := backend.CreatePipe(pipes.CreatePipeInput{
		Name:         "template-pipe",
		RoleARN:      "arn:aws:iam::000000000000:role/r",
		Source:       sqsARN,
		Target:       lambdaARN,
>>>>>>> Stashed changes
		DesiredState: "RUNNING",
		TargetParameters: &pipes.TargetParameters{
			InputTemplate: `{"fixed":"value"}`,
		},
	})
	require.NoError(t, err)
<<<<<<< Updated upstream
	pipes.WaitPipeRunning(t, backend, "template-pipe")
=======
>>>>>>> Stashed changes

	sqsReader := &mockSQSReader{
		messages: []*pipes.SQSMessage{{MessageID: "m1", ReceiptHandle: "rh1", Body: "hello"}},
	}
	lambdaInvoker := &mockPipeLambdaInvoker{}

	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)
	runner.SetLambdaInvoker(lambdaInvoker)

	pipes.PollAllPipesOnce(t.Context(), runner)

	lambdaInvoker.mu.Lock()
	payloads := lambdaInvoker.payloads
	lambdaInvoker.mu.Unlock()

	require.Len(t, payloads, 1)
<<<<<<< Updated upstream
	assert.JSONEq(t, `{"fixed":"value"}`, string(payloads[0]))
=======
	assert.Equal(t, `{"fixed":"value"}`, string(payloads[0]))
>>>>>>> Stashed changes
}
