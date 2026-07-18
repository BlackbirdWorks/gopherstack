package lambda_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// fakeKinesisReader is a no-op KinesisReader for testing.
type fakeKinesisReader struct{}

func (f *fakeKinesisReader) GetShardIDs(_ string) ([]string, error) {
	return nil, nil
}

func (f *fakeKinesisReader) GetShardIterator(_, _, _, _ string) (string, error) {
	return "", nil
}

func (f *fakeKinesisReader) GetRecords(_ string, _ int) ([]lambda.KinesisRecord, string, error) {
	return nil, "", nil
}

// pollingKinesisReader simulates a Kinesis shard with records for poller testing.
type pollingKinesisReader struct {
	shardIDs  []string
	records   []lambda.KinesisRecord
	readCalls int
	mu        sync.Mutex
}

func (r *pollingKinesisReader) GetShardIDs(_ string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.shardIDs, nil
}

func (r *pollingKinesisReader) GetShardIterator(_, _, _, _ string) (string, error) {
	return "test-iterator", nil
}

func (r *pollingKinesisReader) GetRecords(_ string, _ int) ([]lambda.KinesisRecord, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.readCalls++

	if len(r.records) > 0 {
		recs := r.records
		r.records = nil // clear after first read

		return recs, "next-iterator", nil
	}

	return nil, "next-iterator", nil
}

// errorKinesisReader returns errors from GetShardIDs.
type errorKinesisReader struct {
	getShardIDsErr func() error
}

func (r *errorKinesisReader) GetShardIDs(_ string) ([]string, error) {
	return nil, r.getShardIDsErr()
}

func (r *errorKinesisReader) GetShardIterator(_, _, _, _ string) (string, error) {
	return "", nil
}

func (r *errorKinesisReader) GetRecords(_ string, _ int) ([]lambda.KinesisRecord, string, error) {
	return nil, "", nil
}

// fakeSQSReader is a test SQSReader that returns controlled messages and records deletions.
type fakeSQSReader struct {
	receiveErr   error
	deleteErr    error
	messages     []*lambda.SQSMessage
	deletedIDs   []string
	receiveCalls int
	mu           sync.Mutex
}

func (f *fakeSQSReader) ReceiveMessagesLocal(_ string, maxMessages int) ([]*lambda.SQSMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.receiveCalls++

	if f.receiveErr != nil {
		return nil, f.receiveErr
	}

	if len(f.messages) == 0 {
		return nil, nil
	}

	count := min(maxMessages, len(f.messages))
	msgs := f.messages[:count]
	f.messages = f.messages[count:]

	return msgs, nil
}

func (f *fakeSQSReader) DeleteMessagesLocal(_ string, receiptHandles []string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.deleteErr != nil {
		return f.deleteErr
	}

	f.deletedIDs = append(f.deletedIDs, receiptHandles...)

	return nil
}

// getShardIteratorErrorReader is a KinesisReader that fails GetShardIterator.
type getShardIteratorErrorReader struct {
	shardIDs           []string
	shardIteratorCalls int
	mu                 sync.Mutex
}

func (r *getShardIteratorErrorReader) GetShardIDs(_ string) ([]string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.shardIDs, nil
}

func (r *getShardIteratorErrorReader) GetShardIterator(_, _, _, _ string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.shardIteratorCalls++

	return "", assert.AnError
}

func (r *getShardIteratorErrorReader) GetRecords(_ string, _ int) ([]lambda.KinesisRecord, string, error) {
	return nil, "", nil
}

// getRecordsErrorReader is a KinesisReader that succeeds iteration setup but fails GetRecords.
type getRecordsErrorReader struct {
	shardIDs       []string
	getRecordCalls int
	mu             sync.Mutex
}

func (r *getRecordsErrorReader) GetShardIDs(_ string) ([]string, error) {
	return r.shardIDs, nil
}

func (r *getRecordsErrorReader) GetShardIterator(_, _, _, _ string) (string, error) {
	return "test-iter", nil
}

func (r *getRecordsErrorReader) GetRecords(_ string, _ int) ([]lambda.KinesisRecord, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.getRecordCalls++

	return nil, "", assert.AnError
}

// emptyRecordsReader returns an empty records slice with no error.
type emptyRecordsReader struct {
	shardIDs []string
	calls    int
	mu       sync.Mutex
}

func (r *emptyRecordsReader) GetShardIDs(_ string) ([]string, error) {
	return r.shardIDs, nil
}

func (r *emptyRecordsReader) GetShardIterator(_, _, _, _ string) (string, error) {
	return "test-iter", nil
}

func (r *emptyRecordsReader) GetRecords(_ string, _ int) ([]lambda.KinesisRecord, string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls++

	return nil, "next-iter", nil // no error, no records
}

// TestLambda_StartWorker tests that StartWorker can be called.
func TestLambda_StartWorker(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	err := h.StartWorker(ctx)
	assert.NoError(t, err)
}

// TestLambda_SetKinesisPoller tests SetKinesisPoller and StartKinesisPoller.
func TestLambda_SetKinesisPoller(t *testing.T) {
	t.Parallel()

	h, backend := newRealHandler(t)

	ctx, cancel := context.WithTimeout(t.Context(), 200*time.Millisecond)
	defer cancel()

	// Set up a fake KinesisReader
	reader := &fakeKinesisReader{}
	poller := lambda.NewEventSourcePoller(backend, reader)
	backend.SetKinesisPoller(poller)
	backend.StartKinesisPoller(ctx)

	// Should have started the poller - just verify no panic
	err := h.StartWorker(ctx)
	require.NoError(t, err)

	// Let context expire
	<-ctx.Done()
}

// TestLambda_Poller_PollWithRecords tests that the poller invokes Lambda when records arrive.
func TestLambda_Poller_PollWithRecords(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	// Create an ESM
	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-function"}))
	m, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN:   "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
		FunctionName:     "test-function",
		StartingPosition: "TRIM_HORIZON",
		BatchSize:        10,
		Enabled:          true,
	})
	require.NoError(t, err)
	require.NotEmpty(t, m.UUID)

	reader := &pollingKinesisReader{
		shardIDs: []string{"shardId-000000000000"},
		records: []lambda.KinesisRecord{
			{PartitionKey: "pk", SequenceNumber: "1", Data: []byte("hello"), ArrivalTime: time.Now()},
		},
	}

	poller := lambda.NewEventSourcePoller(backend, reader)

	ctx := t.Context()

	// Call poll directly instead of running the goroutine
	lambda.PollOnce(ctx, poller)

	reader.mu.Lock()
	calls := reader.readCalls
	reader.mu.Unlock()

	// Should have polled at least once
	assert.Positive(t, calls)
}

// TestLambda_Poller_PollWithDisabledMapping tests that disabled mappings are skipped.
func TestLambda_Poller_PollWithDisabledMapping(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	// Create a disabled ESM
	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-function"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
		FunctionName:   "test-function",
		Enabled:        false,
	})
	require.NoError(t, err)

	reader := &pollingKinesisReader{
		shardIDs: []string{"shardId-000000000000"},
	}

	poller := lambda.NewEventSourcePoller(backend, reader)
	ctx := t.Context()

	lambda.PollOnce(ctx, poller)

	reader.mu.Lock()
	calls := reader.readCalls
	reader.mu.Unlock()

	// Disabled mapping should not trigger any reads
	assert.Equal(t, 0, calls)
}

// TestLambda_Poller_PollStreamNotFound tests graceful handling of missing streams.
func TestLambda_Poller_PollStreamNotFound(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	// Create an ESM for a non-existent stream
	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-function"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/nonexistent",
		FunctionName:   "test-function",
		Enabled:        true,
	})
	require.NoError(t, err)

	readErrors := 0
	errReader := &errorKinesisReader{getShardIDsErr: func() error {
		readErrors++

		return assert.AnError
	}}

	poller := lambda.NewEventSourcePoller(backend, errReader)
	ctx := t.Context()

	lambda.PollOnce(ctx, poller)

	// Should have attempted to read at least once
	assert.Positive(t, readErrors)
}

// TestLambda_Poller_StartAndStop tests the goroutine lifecycle.
func TestLambda_Poller_StartAndStop(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	reader := &pollingKinesisReader{}
	poller := lambda.NewEventSourcePoller(backend, reader)

	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()

	poller.Start(ctx)
	<-ctx.Done()
	// No panic - goroutine should have stopped cleanly
}

// TestLambda_DeleteEventSourceMapping_RemovesPollerIterators verifies that deleting an ESM
// eagerly removes poller shard iterator state for that mapping.
func TestLambda_DeleteEventSourceMapping_RemovesPollerIterators(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-function"}))
	mapping, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
		FunctionName:   "test-function",
		Enabled:        true,
	})
	require.NoError(t, err)

	reader := &pollingKinesisReader{
		shardIDs: []string{"shardId-000000000000"},
	}

	poller := lambda.NewEventSourcePoller(backend, reader)
	backend.SetKinesisPoller(poller)

	lambda.PollOnce(t.Context(), poller)
	require.Equal(t, 1, lambda.ShardIteratorsLen(poller))

	_, err = backend.DeleteEventSourceMapping(mapping.UUID)
	require.NoError(t, err)
	assert.Zero(t, lambda.ShardIteratorsLen(poller))
}

// TestLambda_Poller_SQS_PollWithMessages_InvocationFails verifies that when Lambda invocation
// fails (e.g. no container runtime available), messages are NOT deleted — preserving them for
// visibility-timeout retry or DLQ processing.
func TestLambda_Poller_SQS_PollWithMessages_InvocationFails(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:test-queue"

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-function"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: queueARN,
		FunctionName:   "test-function",
		BatchSize:      10,
		Enabled:        true,
	})
	require.NoError(t, err)

	reader := &fakeSQSReader{
		messages: []*lambda.SQSMessage{
			{MessageID: "msg-1", ReceiptHandle: "rh-1", Body: "hello"},
			{MessageID: "msg-2", ReceiptHandle: "rh-2", Body: "world"},
		},
	}

	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	poller.SetSQSReader(reader)

	ctx := t.Context()
	lambda.PollOnce(ctx, poller)

	reader.mu.Lock()
	calls := reader.receiveCalls
	deleted := reader.deletedIDs
	reader.mu.Unlock()

	assert.Positive(t, calls, "should have called ReceiveMessagesLocal")
	// Lambda invocation fails (no Docker/port allocator in test backend), so messages
	// must NOT be deleted — they should remain visible for retry/DLQ.
	assert.Empty(t, deleted, "messages must not be deleted when Lambda invocation fails")
}

// TestLambda_Poller_SQS_SkipsDisabledMapping verifies disabled SQS ESMs are not polled.
func TestLambda_Poller_SQS_SkipsDisabledMapping(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:disabled-queue"

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-function"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: queueARN,
		FunctionName:   "test-function",
		BatchSize:      10,
		Enabled:        false,
	})
	require.NoError(t, err)

	reader := &fakeSQSReader{}
	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	poller.SetSQSReader(reader)

	ctx := t.Context()
	lambda.PollOnce(ctx, poller)

	reader.mu.Lock()
	calls := reader.receiveCalls
	reader.mu.Unlock()

	assert.Zero(t, calls, "disabled mapping should not trigger any receives")
}

// TestLambda_Poller_SQS_NoMessagesNoop verifies that empty queues are handled gracefully.
func TestLambda_Poller_SQS_NoMessagesNoop(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:empty-queue"

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-function"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: queueARN,
		FunctionName:   "test-function",
		BatchSize:      10,
		Enabled:        true,
	})
	require.NoError(t, err)

	reader := &fakeSQSReader{messages: nil}
	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	poller.SetSQSReader(reader)

	ctx := t.Context()

	// Should not panic or error on empty queue
	require.NotPanics(t, func() {
		lambda.PollOnce(ctx, poller)
	})

	reader.mu.Lock()
	calls := reader.receiveCalls
	reader.mu.Unlock()

	assert.Positive(t, calls, "should still call ReceiveMessagesLocal even for empty queue")
}

// TestLambda_Poller_SQS_ReceiveError verifies graceful handling of SQS receive errors.
func TestLambda_Poller_SQS_ReceiveError(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:error-queue"

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-function"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: queueARN,
		FunctionName:   "test-function",
		BatchSize:      10,
		Enabled:        true,
	})
	require.NoError(t, err)

	reader := &fakeSQSReader{receiveErr: assert.AnError}
	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	poller.SetSQSReader(reader)

	ctx := t.Context()

	// Should not panic on receive error
	require.NotPanics(t, func() {
		lambda.PollOnce(ctx, poller)
	})
}

// TestLambda_Poller_SQS_IsSQSARN verifies that SQS ARNs are correctly identified.
func TestLambda_Poller_SQS_IsSQSARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		arn   string
		isSQS bool
	}{
		{
			name:  "valid SQS ARN",
			arn:   "arn:aws:sqs:us-east-1:000000000000:my-queue",
			isSQS: true,
		},
		{
			name:  "Kinesis ARN",
			arn:   "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			isSQS: false,
		},
		{
			name:  "empty string",
			arn:   "",
			isSQS: false,
		},
		{
			name:  "Lambda ARN",
			arn:   "arn:aws:lambda:us-east-1:000000000000:function:my-func",
			isSQS: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.isSQS, lambda.IsSQSARN(tc.arn))
		})
	}
}

// TestLambda_Backend_SetSQSReader_NoPoller verifies SetSQSReader is a no-op when no poller is set.
func TestLambda_Backend_SetSQSReader_NoPoller(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	// No poller has been set; SetSQSReader should not panic.
	require.NotPanics(t, func() {
		backend.SetSQSReader(&fakeSQSReader{})
	})
}

// TestLambda_Backend_SetSQSReader_WithPoller verifies that SetSQSReader propagates to the poller.
func TestLambda_Backend_SetSQSReader_WithPoller(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	backend.SetKinesisPoller(poller)

	sqsR := &fakeSQSReader{}
	// Should set the reader on the poller without panicking.
	require.NotPanics(t, func() {
		backend.SetSQSReader(sqsR)
	})

	// Verify the reader was propagated: create an SQS ESM and poll once.
	queueARN := "arn:aws:sqs:us-east-1:000000000000:set-reader-queue"
	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-fn"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: queueARN,
		FunctionName:   "test-fn",
		BatchSize:      10,
		Enabled:        true,
	})
	require.NoError(t, err)

	ctx := t.Context()
	lambda.PollOnce(ctx, poller)

	sqsR.mu.Lock()
	calls := sqsR.receiveCalls
	sqsR.mu.Unlock()

	assert.Positive(t, calls, "SQS reader should have been called after SetSQSReader")
}

// TestLambda_Poller_SQS_DeleteError verifies graceful handling when message deletion fails.
// It stubs Lambda invocation so the poller reaches the delete step, then asserts that the
// delete-error warning is handled without panic.
func TestLambda_Poller_SQS_DeleteError(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:delete-err-queue"

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-function"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: queueARN,
		FunctionName:   "test-function",
		BatchSize:      10,
		Enabled:        true,
	})
	require.NoError(t, err)

	// Supply at least one message so processSQSMapping does not return early.
	reader := &fakeSQSReader{
		messages:  []*lambda.SQSMessage{{MessageID: "msg-1", ReceiptHandle: "rh-1", Body: "body"}},
		deleteErr: assert.AnError,
	}
	poller := lambda.NewEventSourcePoller(backend, &fakeKinesisReader{})
	poller.SetSQSReader(reader)

	// Stub the invoker to return nil so the poller proceeds to the delete step.
	lambda.SetSQSInvoker(poller, func(_ context.Context, _ string) ([]byte, error) { return nil, nil })

	ctx := t.Context()
	// Should not panic even when the delete step fails.
	require.NotPanics(t, func() {
		lambda.PollOnce(ctx, poller)
	})

	// Deletion was attempted: the fakeSQSReader's deleteErr should have been triggered.
	reader.mu.Lock()
	deleted := reader.deletedIDs
	reader.mu.Unlock()

	assert.Empty(t, deleted, "no receipt handles should be appended when deleteErr is set")
}

// TestLambda_Poller_Kinesis_GetShardIteratorError verifies graceful handling of iterator failures.
func TestLambda_Poller_Kinesis_GetShardIteratorError(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-fn"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/err-iter-stream",
		FunctionName:   "test-fn",
		Enabled:        true,
	})
	require.NoError(t, err)

	reader := &getShardIteratorErrorReader{
		shardIDs: []string{"shardId-000000000000"},
	}

	poller := lambda.NewEventSourcePoller(backend, reader)
	ctx := t.Context()

	require.NotPanics(t, func() {
		lambda.PollOnce(ctx, poller)
	})

	reader.mu.Lock()
	calls := reader.shardIteratorCalls
	reader.mu.Unlock()

	assert.Positive(t, calls, "GetShardIterator should have been called")
}

// TestLambda_Poller_Kinesis_GetRecordsError verifies graceful handling of GetRecords failures.
func TestLambda_Poller_Kinesis_GetRecordsError(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-fn"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/get-records-err-stream",
		FunctionName:   "test-fn",
		Enabled:        true,
	})
	require.NoError(t, err)

	reader := &getRecordsErrorReader{
		shardIDs: []string{"shardId-000000000000"},
	}

	poller := lambda.NewEventSourcePoller(backend, reader)
	ctx := t.Context()

	require.NotPanics(t, func() {
		lambda.PollOnce(ctx, poller)
	})

	reader.mu.Lock()
	calls := reader.getRecordCalls
	reader.mu.Unlock()

	assert.Positive(t, calls, "GetRecords should have been called")
}

// TestLambda_Poller_Kinesis_EmptyRecords verifies that empty-record results are handled gracefully.
func TestLambda_Poller_Kinesis_EmptyRecords(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-fn"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/empty-records-stream",
		FunctionName:   "test-fn",
		Enabled:        true,
	})
	require.NoError(t, err)

	reader := &emptyRecordsReader{
		shardIDs: []string{"shardId-000000000000"},
	}

	poller := lambda.NewEventSourcePoller(backend, reader)
	ctx := t.Context()

	require.NotPanics(t, func() {
		lambda.PollOnce(ctx, poller)
	})

	reader.mu.Lock()
	calls := reader.calls
	reader.mu.Unlock()

	assert.Positive(t, calls, "GetRecords should have been called even for empty result")
}

// TestLambda_Poller_NonKinesisNonSQSARN verifies that unknown ESM source types are skipped.
func TestLambda_Poller_NonKinesisNonSQSARN(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	// "arn:aws:dynamodb:us-east-1" has only 4 segments (3 colons), fewer than the 6
	// required by streamNameFromARN, so it returns "" and the mapping is skipped.
	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "test-fn"}))
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:dynamodb:us-east-1",
		FunctionName:   "test-fn",
		Enabled:        true,
	})
	require.NoError(t, err)

	reader := &pollingKinesisReader{shardIDs: []string{"shardId-0"}}
	poller := lambda.NewEventSourcePoller(backend, reader)

	ctx := t.Context()
	require.NotPanics(t, func() {
		lambda.PollOnce(ctx, poller)
	})

	reader.mu.Lock()
	calls := reader.readCalls
	reader.mu.Unlock()

	// The ARN has too few parts → streamNameFromARN returns "" → ESM skipped.
	assert.Zero(t, calls, "non-Kinesis ARN should not trigger GetRecords")
}
