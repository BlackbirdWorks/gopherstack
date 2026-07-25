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

	_, err := backend.CreatePipe(context.Background(), pipes.CreatePipeInput{
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
	pipes.WaitPipeRunning(t, backend, "filter-pipe")

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

	var event map[string]any
	require.NoError(t, json.Unmarshal(payloads[0], &event))
	require.Len(t, event, 1)
	// payload is forwarded - just check Lambda was called once

	deleted := sqsReader.getDeleted()
	assert.Equal(t, []string{"rh1"}, deleted)
}

// TestPipesRunner_ConfigurableBatchSize tests that SourceParameters.BatchSize is used.
func TestPipesRunner_ConfigurableBatchSize(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
	_, err := backend.CreatePipe(context.Background(), pipes.CreatePipeInput{
		Name:         "batch-pipe",
		RoleARN:      "arn:aws:iam::000000000000:role/r",
		Source:       "arn:aws:sqs:us-east-1:000000000000:batch-queue",
		Target:       "arn:aws:lambda:us-east-1:000000000000:function:fn",
		DesiredState: "RUNNING",
		SourceParameters: &pipes.SourceParameters{
			SqsQueueParameters: &pipes.SQSSourceParameters{BatchSize: 3},
		},
	})
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, backend, "batch-pipe")

	sqsReader := &mockSQSReader{}
	runner := pipes.NewRunner(backend)
	runner.SetSQSReader(sqsReader)

	pipes.PollAllPipesOnce(t.Context(), runner)

	assert.Equal(t, 3, sqsReader.getLastMaxMessages(), "runner should request batch size from source parameters")
}

// --- fake mocks + backend factory shared with other runner-family test files ---

// fakeSQSReader is a simple in-process SQS reader for testing.
type fakeSQSReader struct {
	messages []*pipes.SQSMessage
	deleted  []string
}

func (f *fakeSQSReader) ReceivePipeMessages(_ string, maxMsgs int) ([]*pipes.SQSMessage, error) {
	n := min(len(f.messages), maxMsgs)

	out := f.messages[:n]
	f.messages = f.messages[n:]

	return out, nil
}

func (f *fakeSQSReader) DeletePipeMessages(_ string, handles []string) error {
	f.deleted = append(f.deleted, handles...)

	return nil
}

// fakeLambda records invocations.
type fakeLambda struct {
	calls int
}

func (f *fakeLambda) InvokeFunction(
	_ context.Context,
	_, _ string,
	_ []byte,
) ([]byte, int, error) {
	f.calls++

	return []byte(`{}`), 200, nil
}

func newPipeBackend() *pipes.InMemoryBackend {
	return pipes.NewInMemoryBackend("000000000000", "us-east-1")
}

// TestPipeSourceFiltering verifies that messages not matching the filter are dropped.
func TestPipeSourceFiltering(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		filterPattern string
		messages      []string
		wantDelivered int
	}{
		{
			name:          "all_messages_pass_no_filter",
			messages:      []string{`{"event":"a"}`, `{"event":"b"}`},
			filterPattern: "",
			wantDelivered: 2,
		},
		{
			name:          "filter_drops_non_matching",
			messages:      []string{`{"type":"order"}`, `{"type":"payment"}`, `{"type":"order"}`},
			filterPattern: "order",
			wantDelivered: 2,
		},
		{
			name:          "all_filtered_out",
			messages:      []string{`{"type":"payment"}`},
			filterPattern: "order",
			wantDelivered: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newPipeBackend()
			r := pipes.NewRunner(b)

			sqsReader := &fakeSQSReader{}
			lambda := &fakeLambda{}

			for i, body := range tt.messages {
				id := string(rune('a' + i))
				sqsReader.messages = append(sqsReader.messages, &pipes.SQSMessage{
					MessageID:     id,
					ReceiptHandle: "rh-" + id,
					Body:          body,
				})
			}

			r.SetSQSReader(sqsReader)
			r.SetLambdaInvoker(lambda)

			sourceParams := &pipes.SourceParameters{}
			if tt.filterPattern != "" {
				sourceParams.FilterCriteria = &pipes.FilterCriteria{
					Filters: []pipes.Filter{{Pattern: tt.filterPattern}},
				}
			}

			pipeName := "filter-pipe-" + tt.name
			_, err := b.CreatePipe(context.Background(), pipes.CreatePipeInput{
				RoleARN:          "arn:aws:iam::123456789012:role/r",
				Name:             pipeName,
				Source:           "arn:aws:sqs:us-east-1:000000000000:queue",
				Target:           "arn:aws:lambda:us-east-1:000000000000:function:fn",
				DesiredState:     "RUNNING",
				SourceParameters: sourceParams,
			})
			require.NoError(t, err)
			pipes.WaitPipeRunning(t, b, pipeName)

			pipes.PollAllPipesOnce(context.Background(), r)

			if tt.wantDelivered > 0 {
				assert.Positive(t, lambda.calls, "expected Lambda to be invoked")
				assert.Len(t, sqsReader.deleted, tt.wantDelivered, "expected deleted messages count")
			} else {
				assert.Equal(t, 0, lambda.calls, "expected Lambda not to be invoked when all filtered out")
				assert.Empty(t, sqsReader.deleted)
			}
		})
	}
}

// TestPipesRunner_ShardIteratorSweep proves that stale shard iterator cache
// entries are pruned once their pipe is no longer RUNNING, bounding the cache
// instead of growing it unboundedly for every stopped/deleted stream pipe.
// The cache is unexported, so this drives the real background poll loop
// (Runner.Start, which ticks and sweeps on its own) through a stop/restart
// cycle and observes the prune indirectly: a pruned iterator forces a fresh
// GetShardIterator call the next time the pipe is polled, whereas a leaked
// (un-pruned) iterator would silently be reused forever without ever calling
// GetShardIterator again.
func TestPipesRunner_ShardIteratorSweep(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
	kinesisARN := "arn:aws:kinesis:us-east-1:000000000000:stream/sweep-stream"
	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	createTestPipe(t, backend, "sweep-pipe", kinesisARN, lambdaARN, "RUNNING")

	reader := &fakeKinesisReader{
		shardIDs: []string{"shard-1"},
		pending:  map[string][]pipes.KinesisRecord{},
	}
	runner := pipes.NewRunner(backend)
	runner.SetKinesisReader(reader)

	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	runner.Start(ctx)

	getIterCalls := func() int {
		reader.mu.Lock()
		defer reader.mu.Unlock()

		return reader.getIterCalls
	}

	// The runner's first background tick polls the running pipe and caches
	// one shard iterator for it.
	require.Eventually(t, func() bool { return getIterCalls() >= 1 }, 3*time.Second, 20*time.Millisecond,
		"expected the runner's first tick to request a shard iterator for the running pipe")

	_, err := backend.StopPipe(context.Background(), "sweep-pipe")
	require.NoError(t, err)
	pipes.WaitPipeStopped(t, backend, "sweep-pipe")

	// Give the background ticker at least one full cycle while the pipe is
	// stopped, so the sweep observes it outside the running set and prunes
	// its cached shard iterator (the pipe itself is not polled while
	// stopped, so this cannot be observed until it runs again below).
	time.Sleep(1500 * time.Millisecond)

	_, err = backend.StartPipe(context.Background(), "sweep-pipe")
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, backend, "sweep-pipe")

	require.Eventually(t, func() bool { return getIterCalls() >= 2 }, 3*time.Second, 20*time.Millisecond,
		"expected a fresh GetShardIterator call after the pipe restarted, proving the sweep "+
			"pruned the stale cache entry while the pipe was stopped")
}

// TestPipesRunner_InputTemplate tests that TargetParameters.InputTemplate overrides default payload.
func TestPipesRunner_InputTemplate(t *testing.T) {
	t.Parallel()

	backend := newTestPipeBackend(t)
	_, err := backend.CreatePipe(context.Background(), pipes.CreatePipeInput{
		Name:         "template-pipe",
		RoleARN:      "arn:aws:iam::000000000000:role/r",
		Source:       "arn:aws:sqs:us-east-1:000000000000:tmpl-queue",
		Target:       "arn:aws:lambda:us-east-1:000000000000:function:fn",
		DesiredState: "RUNNING",
		TargetParameters: &pipes.TargetParameters{
			InputTemplate: `{"fixed":"value"}`,
		},
	})
	require.NoError(t, err)
	pipes.WaitPipeRunning(t, backend, "template-pipe")

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
	assert.JSONEq(t, `{"fixed":"value"}`, string(payloads[0]))
}
