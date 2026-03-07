package lambda_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lambda"
)

// newRealHandler creates a lambda.Handler backed by a real InMemoryBackend for ESM tests.
func newRealHandler(t *testing.T) (*lambda.Handler, *lambda.InMemoryBackend) {
	t.Helper()

	backend := lambda.NewInMemoryBackend(
		nil, nil, lambda.DefaultSettings(),
		"000000000000", "us-east-1",
	)
	handler := lambda.NewHandler(backend)

	return handler, backend
}

// doESMRequest sends an HTTP request to the ESM endpoint.
func doESMRequest(t *testing.T, h *lambda.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// TestLambda_ESM_CRUD tests the full Create / Get / List / Delete lifecycle.
func TestLambda_ESM_CRUD(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream"

	// CreateEventSourceMapping
	rec := doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"EventSourceArn":   streamARN,
		"FunctionName":     "my-function",
		"StartingPosition": "TRIM_HORIZON",
		"BatchSize":        50,
		"Enabled":          true,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		UUID           string `json:"UUID"`
		EventSourceARN string `json:"EventSourceArn"`
		State          string `json:"State"`
		BatchSize      int    `json:"BatchSize"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.NotEmpty(t, createResp.UUID)
	assert.Equal(t, streamARN, createResp.EventSourceARN)
	assert.Equal(t, "Enabled", createResp.State)
	assert.Equal(t, 50, createResp.BatchSize)

	esmUUID := createResp.UUID

	// GetEventSourceMapping
	rec = doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/"+esmUUID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		UUID  string `json:"UUID"`
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Equal(t, esmUUID, getResp.UUID)

	// ListEventSourceMappings
	rec = doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		EventSourceMappings []struct {
			UUID string `json:"UUID"`
		} `json:"EventSourceMappings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	require.Len(t, listResp.EventSourceMappings, 1)
	assert.Equal(t, esmUUID, listResp.EventSourceMappings[0].UUID)

	// DeleteEventSourceMapping
	rec = doESMRequest(t, h, http.MethodDelete, "/2015-03-31/event-source-mappings/"+esmUUID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify gone
	rec = doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/"+esmUUID, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestLambda_ESM_CreateDisabled tests creating a disabled event source mapping.
func TestLambda_ESM_CreateDisabled(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	enabled := false
	rec := doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"EventSourceArn": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
		"FunctionName":   "my-function",
		"Enabled":        enabled,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Disabled", resp.State)
}

// TestLambda_ESM_CreateNilEnabled tests that omitting Enabled defaults to true.
func TestLambda_ESM_CreateNilEnabled(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	// No Enabled field - should default to enabled
	rec := doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"EventSourceArn": "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
		"FunctionName":   "my-function",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp struct {
		State string `json:"State"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "Enabled", resp.State)
}

// TestLambda_ESM_ListByFunctionName tests filtering ListEventSourceMappings by function name.
func TestLambda_ESM_ListByFunctionName(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	// Create two mappings for different functions
	doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"EventSourceArn": "arn:aws:kinesis:us-east-1:000000000000:stream/stream-1",
		"FunctionName":   "function-a",
	})
	doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", map[string]any{
		"EventSourceArn": "arn:aws:kinesis:us-east-1:000000000000:stream/stream-2",
		"FunctionName":   "function-b",
	})

	// List all
	rec := doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var allResp struct {
		EventSourceMappings []any `json:"EventSourceMappings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &allResp))
	assert.Len(t, allResp.EventSourceMappings, 2)

	// List for function-a only
	rec = doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/?FunctionName=function-a", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var filtResp struct {
		EventSourceMappings []struct {
			FunctionARN string `json:"FunctionArn"`
		} `json:"EventSourceMappings"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &filtResp))
	assert.Len(t, filtResp.EventSourceMappings, 1)
	assert.Contains(t, filtResp.EventSourceMappings[0].FunctionARN, "function-a")
}

// TestLambda_ESM_GetNotFound tests getting a non-existent ESM.
func TestLambda_ESM_GetNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	rec := doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/nonexistent-uuid", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestLambda_ESM_DeleteNotFound tests deleting a non-existent ESM.
func TestLambda_ESM_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	rec := doESMRequest(t, h, http.MethodDelete, "/2015-03-31/event-source-mappings/nonexistent-uuid", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
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

// TestLambda_ESM_InvalidJSON tests that CreateESM rejects invalid JSON.
func TestLambda_ESM_InvalidJSON(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost,
		"/2015-03-31/event-source-mappings/",
		bytes.NewReader([]byte("{invalid")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestLambda_ESM_RouteMatcher tests ESM routes are matched.
func TestLambda_ESM_RouteMatcher(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)
	e := echo.New()

	// ESM path should be matched
	req := httptest.NewRequest(http.MethodPost, "/2015-03-31/event-source-mappings/", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c))

	// Lambda function path should still be matched
	req2 := httptest.NewRequest(http.MethodGet, "/2015-03-31/functions", nil)
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c2))
}

// TestLambda_ESM_UnknownESMMethod tests that an unknown method returns 404.
func TestLambda_ESM_UnknownESMMethod(t *testing.T) {
	t.Parallel()

	h, _ := newRealHandler(t)

	// PUT is not supported for ESM
	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPut,
		"/2015-03-31/event-source-mappings/some-uuid",
		bytes.NewReader([]byte("{}")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestLambda_StreamNameFromARN tests the ARN parser.
func TestLambda_StreamNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		expected string
	}{
		{
			name:     "valid Kinesis ARN",
			arn:      "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
			expected: "my-stream",
		},
		{
			name:     "valid ARN with hyphens",
			arn:      "arn:aws:kinesis:eu-west-1:123456789012:stream/my-test-stream",
			expected: "my-test-stream",
		},
		{
			name:     "empty string",
			arn:      "",
			expected: "",
		},
		{
			name:     "invalid ARN",
			arn:      "not-an-arn",
			expected: "",
		},
		{
			name:     "too short ARN",
			arn:      "arn:aws",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := lambda.StreamNameFromARN(tc.arn)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// TestLambda_FunctionNameFromARN tests the Lambda ARN function name parser.
func TestLambda_FunctionNameFromARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		arn      string
		expected string
	}{
		{
			name:     "valid Lambda ARN",
			arn:      "arn:aws:lambda:us-east-1:000000000000:function:my-function",
			expected: "my-function",
		},
		{
			name:     "empty string",
			arn:      "",
			expected: "",
		},
		{
			name:     "just a name",
			arn:      "my-function",
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := lambda.FunctionNameFromARN(tc.arn)
			assert.Equal(t, tc.expected, result)
		})
	}
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

// TestLambda_Poller_PollWithRecords tests that the poller invokes Lambda when records arrive.
func TestLambda_Poller_PollWithRecords(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	// Create an ESM
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

// TestLambda_Poller_SQS_PollWithMessages verifies that the SQS poller delivers messages to Lambda
// and deletes them on success.
func TestLambda_Poller_SQS_PollWithMessages(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:test-queue"

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
	// Lambda invocation will fail (no Docker), so messages should NOT be deleted
	assert.Empty(t, deleted, "messages should not be deleted when Lambda invocation fails")
}

// TestLambda_Poller_SQS_SkipsDisabledMapping verifies disabled SQS ESMs are not polled.
func TestLambda_Poller_SQS_SkipsDisabledMapping(t *testing.T) {
	t.Parallel()

	_, backend := newRealHandler(t)

	queueARN := "arn:aws:sqs:us-east-1:000000000000:disabled-queue"

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
