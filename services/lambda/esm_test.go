package lambda_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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
	closeBackend(t, backend)
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

// TestLambda_ESM_FilterCriteria_RoundTrip verifies that FilterCriteria submitted
// at create / update time is preserved across Get and List operations for AWS parity.
func TestLambda_ESM_FilterCriteria_RoundTrip(t *testing.T) {
	t.Parallel()

	h, bk := newRealHandler(t)

	streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream-fc"
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "fc-fn"}))

	createBody := map[string]any{
		"EventSourceArn":   streamARN,
		"FunctionName":     "fc-fn",
		"StartingPosition": "TRIM_HORIZON",
		"BatchSize":        10,
		"Enabled":          true,
		"FilterCriteria": map[string]any{
			"Filters": []map[string]string{
				{"Pattern": `{"data":{"orderType":["premium"]}}`},
			},
		},
	}

	rec := doESMRequest(t, h, http.MethodPost, "/2015-03-31/event-source-mappings/", createBody)
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp struct {
		UUID           string `json:"UUID"`
		FilterCriteria struct {
			Filters []struct {
				Pattern string `json:"Pattern"`
			} `json:"Filters"`
		} `json:"FilterCriteria"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	require.NotEmpty(t, createResp.UUID)
	require.Len(t, createResp.FilterCriteria.Filters, 1)
	assert.JSONEq(t, `{"data":{"orderType":["premium"]}}`, createResp.FilterCriteria.Filters[0].Pattern)

	// Update with new filter pattern.
	updBody := map[string]any{
		"FilterCriteria": map[string]any{
			"Filters": []map[string]string{
				{"Pattern": `{"data":{"orderType":["standard"]}}`},
			},
		},
	}
	rec = doESMRequest(t, h, http.MethodPut, "/2015-03-31/event-source-mappings/"+createResp.UUID, updBody)
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doESMRequest(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/"+createResp.UUID, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		FilterCriteria struct {
			Filters []struct {
				Pattern string `json:"Pattern"`
			} `json:"Filters"`
		} `json:"FilterCriteria"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	require.Len(t, getResp.FilterCriteria.Filters, 1)
	assert.JSONEq(t, `{"data":{"orderType":["standard"]}}`, getResp.FilterCriteria.Filters[0].Pattern)
}

// TestLambda_ESM_CRUD tests the full Create / Get / List / Delete lifecycle.
func TestLambda_ESM_CRUD(t *testing.T) {
	t.Parallel()

	h, bk := newRealHandler(t)

	streamARN := "arn:aws:kinesis:us-east-1:000000000000:stream/my-stream"

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "my-function"}))

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

	h, bk := newRealHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "my-function"}))

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

	h, bk := newRealHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "my-function"}))

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

	h, bk := newRealHandler(t)

	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "function-a"}))
	require.NoError(t, bk.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "function-b"}))

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
		{
			// Passes the len(arn)>len(prefix) check but has too few colon-separated parts.
			name:     "too few parts after prefix",
			arn:      "arn:aws:kinesis:us-east-1",
			expected: "",
		},
		{
			// Has 6 parts but the last segment is just "stream/" with no stream name.
			name:     "stream segment too short",
			arn:      "arn:aws:kinesis:us-east-1:000000000000:stream/",
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
			// Bug fix (parity-sweep-3): the old implementation SplitN'd on ":"
			// with a fixed part count and returned "" for any input shorter
			// than a full ARN, silently dropping bare function names. A bare
			// name is a legitimate input (callers fall back to it when the
			// ARN doesn't parse) and must round-trip unchanged.
			name:     "just a name",
			arn:      "my-function",
			expected: "my-function",
		},
		{
			// Qualified ARN: the qualifier segment must be stripped, not
			// glued onto the name (previously "arn:...:function:my-func:PROD"
			// incorrectly returned "PROD" as the "name").
			name:     "qualified ARN with alias",
			arn:      "arn:aws:lambda:us-east-1:000000000000:function:my-function:PROD",
			expected: "my-function",
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

// TestLambda_ESMIndex_ListByFunctionName_UsesIndex verifies that the ESM function-ARN
// index correctly filters mappings per function and that listing with no filter returns all.
func TestLambda_ESMIndex_ListByFunctionName_UsesIndex(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterFn     string
		wantContains string
		wantCount    int
	}{
		{
			name:         "filter_function_a",
			filterFn:     "function-a",
			wantCount:    1,
			wantContains: "function-a",
		},
		{
			name:         "filter_function_b",
			filterFn:     "function-b",
			wantCount:    1,
			wantContains: "function-b",
		},
		{
			name:      "no_filter_returns_all",
			filterFn:  "",
			wantCount: 2,
		},
	}

	_, backend := newRealHandler(t)

	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "function-a"}))
	require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "function-b"}))

	// Create ESMs for two distinct functions.
	_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/stream-a",
		FunctionName:   "function-a",
		Enabled:        true,
	})
	require.NoError(t, err)

	_, err = backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
		EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/stream-b",
		FunctionName:   "function-b",
		Enabled:        true,
	})
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := backend.ListEventSourceMappings(tt.filterFn, "", "", 0)
			assert.Len(t, result.Data, tt.wantCount)

			if tt.wantContains != "" {
				require.Len(t, result.Data, 1)
				assert.Contains(t, result.Data[0].FunctionARN, tt.wantContains)
			}
		})
	}
}

// TestLambda_DeleteFunction_CascadesESMDelete verifies that deleting a function also
// removes all of its event source mappings.
func TestLambda_DeleteFunction_CascadesESMDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "cascade_deletes_esm"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			fnName := "cascade-fn-" + tt.name
			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: fnName}))

			_, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/cascade-stream",
				FunctionName:   fnName,
				Enabled:        true,
			})
			require.NoError(t, err)

			// Verify ESM exists before deletion.
			before := backend.ListEventSourceMappings(fnName, "", "", 0)
			require.Len(t, before.Data, 1)

			// Delete the function.
			require.NoError(t, backend.DeleteFunction(fnName))

			// ESMs for the deleted function must be gone.
			after := backend.ListEventSourceMappings(fnName, "", "", 0)
			assert.Empty(t, after.Data)

			// The global list must also be empty.
			all := backend.ListEventSourceMappings("", "", "", 0)
			assert.Empty(t, all.Data)
		})
	}
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

// TestLambda_CreateESM_AllowsNonExistentFunction verifies that CreateEventSourceMapping
// succeeds even when the referenced Lambda function does not exist. AWS allows creating ESMs
// for functions that have not yet been created; the ESM enters an error state at invoke time.
func TestLambda_CreateESM_AllowsNonExistentFunction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		functionName string
	}{
		{
			name:         "bare_function_name",
			functionName: "does-not-exist",
		},
		{
			name:         "function_arn",
			functionName: "arn:aws:lambda:us-east-1:000000000000:function:also-does-not-exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			m, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/test-stream",
				FunctionName:   tt.functionName,
				Enabled:        true,
			})

			require.NoError(t, err, "CreateEventSourceMapping must succeed even when function does not exist")
			assert.NotEmpty(t, m.UUID)
		})
	}
}

// TestLambda_UpdateESM_UpdatesLastModified verifies that UpdateEventSourceMapping sets
// LastModified to a time after the mapping was created.
func TestLambda_UpdateESM_UpdatesLastModified(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "updates_last_modified"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, backend := newRealHandler(t)

			require.NoError(t, backend.CreateFunction(&lambda.FunctionConfiguration{FunctionName: "update-esm-fn"}))

			m, err := backend.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
				EventSourceARN: "arn:aws:kinesis:us-east-1:000000000000:stream/update-stream",
				FunctionName:   "update-esm-fn",
				Enabled:        true,
			})
			require.NoError(t, err)

			createdAt := m.LastModified

			// Ensure at least 1ms passes.
			time.Sleep(time.Millisecond)

			updated, updateErr := backend.UpdateEventSourceMapping(m.UUID, &lambda.UpdateEventSourceMappingInput{
				Enabled:   new(false),
				BatchSize: 0,
			})
			require.NoError(t, updateErr)

			assert.True(t, updated.LastModified.After(createdAt),
				"LastModified should be after creation time: got %v, created %v", updated.LastModified, createdAt)
		})
	}
}

// --- EventSourceMapping edge cases ---

func TestBatch1_ESM_ListByFunctionName(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	fnName := "esm-fn"
	createFunctionForTest(t, h, fnName)

	// Create ESM
	rec := callInMemoryHandler(
		t, h, http.MethodPost,
		"/2015-03-31/event-source-mappings",
		`{"FunctionName":"esm-fn",`+
			`"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",`+
			`"StartingPosition":"LATEST"}`,
	)
	require.Equal(t, http.StatusCreated, rec.Code)

	var created map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &created))
	uuid := created["UUID"].(string)

	// List by function name
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings?FunctionName=esm-fn", "{}")
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), uuid)

	// Get ESM
	rec = callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings/"+uuid, "{}")
	require.Equal(t, http.StatusOK, rec.Code)

	// Update ESM
	rec = callInMemoryHandler(
		t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"BatchSize":50}`,
	)
	require.Equal(t, http.StatusOK, rec.Code)

	// Delete ESM
	rec = callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/event-source-mappings/"+uuid, "{}")
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ============================================================
// EventSourceMapping: batch + filter + window parameters
// ============================================================

func TestBatch2_ESM_BatchWindow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-batch-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/my-stream",
		"FunctionName":"esm-batch-fn",
		"StartingPosition":"LATEST",
		"BatchSize":100,
		"MaximumBatchingWindowInSeconds":60
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(60), out["MaximumBatchingWindowInSeconds"], 0)
}

func TestBatch2_ESM_TumblingWindow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-tumble-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/tumble-stream",
		"FunctionName":"esm-tumble-fn",
		"StartingPosition":"LATEST",
		"TumblingWindowInSeconds":300
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(300), out["TumblingWindowInSeconds"], 0)
}

func TestBatch2_ESM_FilterCriteria(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-filter-fn")

	body := `{
		"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:my-queue",
		"FunctionName":"esm-filter-fn",
		"FilterCriteria":{"Filters":[{"Pattern":"{\"source\":[\"myapp\"]}"}]}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	fc, ok := out["FilterCriteria"].(map[string]any)
	require.True(t, ok)
	filters := fc["Filters"].([]any)
	require.Len(t, filters, 1)
}

func TestBatch2_ESM_BisectBatchOnError(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-bisect-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/bisect-stream",
		"FunctionName":"esm-bisect-fn",
		"StartingPosition":"LATEST",
		"BisectBatchOnFunctionError":true
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.Equal(t, true, out["BisectBatchOnFunctionError"])
}

func TestBatch2_ESM_MaximumRecordAge(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-recage-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/age-stream",
		"FunctionName":"esm-recage-fn",
		"StartingPosition":"LATEST",
		"MaximumRecordAgeInSeconds":3600
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(3600), out["MaximumRecordAgeInSeconds"], 0)
}

func TestBatch2_ESM_ParallelizationFactor(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-parallel-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/parallel-stream",
		"FunctionName":"esm-parallel-fn",
		"StartingPosition":"LATEST",
		"ParallelizationFactor":5
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(5), out["ParallelizationFactor"], 0)
}

func TestBatch2_ESM_DestinationConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-dest-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/dest-stream",
		"FunctionName":"esm-dest-fn",
		"StartingPosition":"LATEST",
		"DestinationConfig":{"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:dead-letter"}}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	dc, ok := out["DestinationConfig"].(map[string]any)
	require.True(t, ok)
	onFail := dc["OnFailure"].(map[string]any)
	assert.Equal(t, "arn:aws:sqs:us-east-1:000000000000:dead-letter", onFail["Destination"])
}

func TestBatch2_ESM_UpdateBatchWindow(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-upd-window-fn")

	createBody := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/upd-stream",
		"FunctionName":"esm-upd-window-fn",
		"StartingPosition":"LATEST"
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", createBody)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	uuid := created["UUID"].(string)

	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"MaximumBatchingWindowInSeconds":30}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var updated map[string]any
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updated))
	assert.InDelta(t, float64(30), updated["MaximumBatchingWindowInSeconds"], 0)
}

func TestBatch2_ESM_SQS_FullOptions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-sqs-fn")

	body := `{
		"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:my-sqs-queue",
		"FunctionName":"esm-sqs-fn",
		"BatchSize":10,
		"MaximumBatchingWindowInSeconds":5,
		"FilterCriteria":{"Filters":[{"Pattern":"{\"body\":{\"type\":[\"order\"]}}"}]},
		"DestinationConfig":{"OnFailure":{"Destination":"arn:aws:sqs:us-east-1:000000000000:sqs-dlq"}}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(10), out["BatchSize"], 0)
	assert.InDelta(t, float64(5), out["MaximumBatchingWindowInSeconds"], 0)
	assert.NotNil(t, out["FilterCriteria"])
	assert.NotNil(t, out["DestinationConfig"])
	assert.Equal(t, "Enabled", out["State"])
}

func TestBatch2_ESM_DDB_FullOptions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-ddb-fn")

	body := `{
		"EventSourceArn":"arn:aws:dynamodb:us-east-1:000000000000:table/my-table/stream/2024-01-01T00:00:00.000",
		"FunctionName":"esm-ddb-fn",
		"StartingPosition":"LATEST",
		"BatchSize":50,
		"BisectBatchOnFunctionError":true,
		"MaximumRetryAttempts":3
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(50), out["BatchSize"], 0)
	assert.Equal(t, true, out["BisectBatchOnFunctionError"])
	assert.InDelta(t, float64(3), out["MaximumRetryAttempts"], 0)
}

func TestBatch2_ESM_Kinesis_AllWindowOptions(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-kin-fn")

	body := `{
		"EventSourceArn":"arn:aws:kinesis:us-east-1:000000000000:stream/full-stream",
		"FunctionName":"esm-kin-fn",
		"StartingPosition":"TRIM_HORIZON",
		"BatchSize":200,
		"MaximumBatchingWindowInSeconds":120,
		"TumblingWindowInSeconds":60,
		"MaximumRecordAgeInSeconds":7200,
		"MaximumRetryAttempts":5,
		"ParallelizationFactor":3,
		"BisectBatchOnFunctionError":true
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	assert.InDelta(t, float64(200), out["BatchSize"], 0)
	assert.InDelta(t, float64(120), out["MaximumBatchingWindowInSeconds"], 0)
	assert.InDelta(t, float64(60), out["TumblingWindowInSeconds"], 0)
	assert.InDelta(t, float64(7200), out["MaximumRecordAgeInSeconds"], 0)
	assert.InDelta(t, float64(5), out["MaximumRetryAttempts"], 0)
	assert.InDelta(t, float64(3), out["ParallelizationFactor"], 0)
	assert.Equal(t, true, out["BisectBatchOnFunctionError"])
}

func TestBatch2_ESM_ListByFunction(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-list-fn")

	for i := range 3 {
		body := fmt.Sprintf(`{
			"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:q-%d",
			"FunctionName":"esm-list-fn"
		}`, i)
		callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	}

	rec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings?FunctionName=esm-list-fn", "")
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	mappings := out["EventSourceMappings"].([]any)
	assert.Len(t, mappings, 3)
}

// ============================================================
// ESM: enable / disable state transitions
// ============================================================

func TestBatch2_ESM_EnableDisable(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-toggle-fn")

	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/event-source-mappings",
		`{"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:toggle-q","FunctionName":"esm-toggle-fn"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	uuid := created["UUID"].(string)

	// Disable
	disabled := false
	_ = disabled
	updRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"Enabled":false}`)
	require.Equal(t, http.StatusOK, updRec.Code)

	var updOut map[string]any
	require.NoError(t, json.NewDecoder(updRec.Body).Decode(&updOut))
	assert.Equal(t, "Disabled", updOut["State"])

	// Re-enable
	enableRec := callInMemoryHandler(t, h, http.MethodPut,
		"/2015-03-31/event-source-mappings/"+uuid,
		`{"Enabled":true}`)
	require.Equal(t, http.StatusOK, enableRec.Code)

	var enableOut map[string]any
	require.NoError(t, json.NewDecoder(enableRec.Body).Decode(&enableOut))
	assert.Equal(t, "Enabled", enableOut["State"])
}

func TestBatch2_ESM_DeleteReturnsMapping(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "esm-del-fn")

	createRec := callInMemoryHandler(t, h, http.MethodPost,
		"/2015-03-31/event-source-mappings",
		`{"EventSourceArn":"arn:aws:sqs:us-east-1:000000000000:del-q","FunctionName":"esm-del-fn"}`)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	uuid := created["UUID"].(string)

	delRec := callInMemoryHandler(t, h, http.MethodDelete,
		"/2015-03-31/event-source-mappings/"+uuid, "")
	require.Equal(t, http.StatusOK, delRec.Code)

	var delOut map[string]any
	require.NoError(t, json.NewDecoder(delRec.Body).Decode(&delOut))
	assert.Equal(t, uuid, delOut["UUID"])

	// Get after delete → 404
	getRec := callInMemoryHandler(t, h, http.MethodGet,
		"/2015-03-31/event-source-mappings/"+uuid, "")
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

// ============================================================
// Amazon MSK event source
// ============================================================

func TestBatch3_ESM_MSK_Topics_And_ConsumerGroup(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "msk-fn")

	body := `{
		"FunctionName":"msk-fn",
		"EventSourceArn":"arn:aws:kafka:us-east-1:000000000000:cluster/my-cluster/abc",
		"Topics":["my-topic"],
		"StartingPosition":"TRIM_HORIZON",
		"AmazonManagedKafkaEventSourceConfig":{"ConsumerGroupId":"my-group"}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	topics, _ := out["Topics"].([]any)
	require.Len(t, topics, 1)
	assert.Equal(t, "my-topic", topics[0])

	cfg, _ := out["AmazonManagedKafkaEventSourceConfig"].(map[string]any)
	require.NotNil(t, cfg)
	assert.Equal(t, "my-group", cfg["ConsumerGroupId"])
}

func TestBatch3_ESM_MSK_Get_PreservesConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "msk-get-fn")

	body := `{
		"FunctionName":"msk-get-fn",
		"EventSourceArn":"arn:aws:kafka:us-east-1:000000000000:cluster/my-cluster/abc",
		"Topics":["topic-a"],
		"AmazonManagedKafkaEventSourceConfig":{"ConsumerGroupId":"group-1"}
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	id, _ := created["UUID"].(string)
	require.NotEmpty(t, id)

	getRec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var got map[string]any
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&got))

	topics, _ := got["Topics"].([]any)
	require.Len(t, topics, 1)
	assert.Equal(t, "topic-a", topics[0])

	cfg, _ := got["AmazonManagedKafkaEventSourceConfig"].(map[string]any)
	require.NotNil(t, cfg)
	assert.Equal(t, "group-1", cfg["ConsumerGroupId"])
}

// ============================================================
// Self-managed Apache Kafka event source
// ============================================================

func TestBatch3_ESM_SelfManagedKafka_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "smk-fn")

	// SelfManaged Kafka has no EventSourceArn in real AWS; use a placeholder to satisfy backend validation.
	body := `{
		"FunctionName":"smk-fn",
		"EventSourceArn":"arn:aws:kafka:us-east-1:000000000000:cluster/smk/xyz",
		"Topics":["smk-topic"],
		"SelfManagedEventSource":{
			"Endpoints":{"KAFKA_BOOTSTRAP_SERVERS":["broker1:9092","broker2:9092"]}
		},
		"SelfManagedKafkaEventSourceConfig":{"ConsumerGroupId":"smk-group"},
		"SourceAccessConfigurations":[
			{"Type":"SASL_SCRAM_512_AUTH","URI":"arn:aws:secretsmanager:us-east-1:000000000000:secret:kafka-secret"}
		]
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	topics, _ := out["Topics"].([]any)
	require.Len(t, topics, 1)
	assert.Equal(t, "smk-topic", topics[0])

	sme, _ := out["SelfManagedEventSource"].(map[string]any)
	require.NotNil(t, sme)
	endpoints, _ := sme["Endpoints"].(map[string]any)
	brokers, _ := endpoints["KAFKA_BOOTSTRAP_SERVERS"].([]any)
	assert.Len(t, brokers, 2)

	smk, _ := out["SelfManagedKafkaEventSourceConfig"].(map[string]any)
	require.NotNil(t, smk)
	assert.Equal(t, "smk-group", smk["ConsumerGroupId"])

	sacs, _ := out["SourceAccessConfigurations"].([]any)
	require.Len(t, sacs, 1)
	sac, _ := sacs[0].(map[string]any)
	assert.Equal(t, "SASL_SCRAM_512_AUTH", sac["Type"])
	assert.Equal(t, "arn:aws:secretsmanager:us-east-1:000000000000:secret:kafka-secret", sac["URI"])
}

func TestBatch3_ESM_SelfManagedKafka_UpdateSourceAccessConfigs(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "smk-upd-fn")

	createBody := `{
		"FunctionName":"smk-upd-fn",
		"EventSourceArn":"arn:aws:kafka:us-east-1:000000000000:cluster/smk/upd",
		"Topics":["t1"],
		"SelfManagedKafkaEventSourceConfig":{"ConsumerGroupId":"g1"}
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", createBody)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	id := created["UUID"].(string)

	updateBody := `{
		"SourceAccessConfigurations":[
			{"Type":"VPC_SUBNET","URI":"subnet-abc"},
			{"Type":"VPC_SECURITY_GROUP","URI":"sg-abc"}
		]
	}`
	updateRec := callInMemoryHandler(t, h, http.MethodPut, "/2015-03-31/event-source-mappings/"+id, updateBody)
	require.Equal(t, http.StatusOK, updateRec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(updateRec.Body).Decode(&out))

	sacs, _ := out["SourceAccessConfigurations"].([]any)
	assert.Len(t, sacs, 2)
}

// ============================================================
// Amazon MQ / Queues
// ============================================================

func TestBatch3_ESM_AmazonMQ_Queues(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "mq-fn")

	body := `{
		"FunctionName":"mq-fn",
		"EventSourceArn":"arn:aws:mq:us-east-1:000000000000:broker:my-broker:b-abc",
		"Queues":["TestQueue"],
		"SourceAccessConfigurations":[
			{"Type":"BASIC_AUTH","URI":"arn:aws:secretsmanager:us-east-1:000000000000:secret:mq-creds"}
		]
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	queues, _ := out["Queues"].([]any)
	require.Len(t, queues, 1)
	assert.Equal(t, "TestQueue", queues[0])

	sacs, _ := out["SourceAccessConfigurations"].([]any)
	require.Len(t, sacs, 1)
}

// ============================================================
// Amazon DocumentDB event source
// ============================================================

func TestBatch3_ESM_DocumentDB_Create(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "docdb-fn")

	body := `{
		"FunctionName":"docdb-fn",
		"EventSourceArn":"arn:aws:rds:us-east-1:000000000000:cluster:my-docdb-cluster",
		"StartingPosition":"LATEST",
		"DocumentDBEventSourceConfig":{
			"DatabaseName":"mydb",
			"CollectionName":"mycoll",
			"FullDocument":"UpdateLookup"
		}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	docdb, _ := out["DocumentDBEventSourceConfig"].(map[string]any)
	require.NotNil(t, docdb)
	assert.Equal(t, "mydb", docdb["DatabaseName"])
	assert.Equal(t, "mycoll", docdb["CollectionName"])
	assert.Equal(t, "UpdateLookup", docdb["FullDocument"])
}

func TestBatch3_ESM_DocumentDB_Get_PreservesConfig(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "docdb-get-fn")

	body := `{
		"FunctionName":"docdb-get-fn",
		"EventSourceArn":"arn:aws:rds:us-east-1:000000000000:cluster:my-cluster",
		"DocumentDBEventSourceConfig":{
			"DatabaseName":"testdb",
			"FullDocument":"Default"
		}
	}`
	createRec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, createRec.Code)

	var created map[string]any
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&created))
	id := created["UUID"].(string)

	getRec := callInMemoryHandler(t, h, http.MethodGet, "/2015-03-31/event-source-mappings/"+id, "")
	require.Equal(t, http.StatusOK, getRec.Code)

	var got map[string]any
	require.NoError(t, json.NewDecoder(getRec.Body).Decode(&got))

	docdb, _ := got["DocumentDBEventSourceConfig"].(map[string]any)
	require.NotNil(t, docdb)
	assert.Equal(t, "testdb", docdb["DatabaseName"])
	assert.Equal(t, "Default", docdb["FullDocument"])
}

func TestBatch3_ESM_DocumentDB_NoCollectionName_Omitted(t *testing.T) {
	t.Parallel()

	h, _ := newInMemoryHandler(t)
	createFunctionForTest(t, h, "docdb-nocoll-fn")

	body := `{
		"FunctionName":"docdb-nocoll-fn",
		"EventSourceArn":"arn:aws:rds:us-east-1:000000000000:cluster:c",
		"DocumentDBEventSourceConfig":{"DatabaseName":"db1"}
	}`
	rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
	require.Equal(t, http.StatusCreated, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

	docdb, _ := out["DocumentDBEventSourceConfig"].(map[string]any)
	require.NotNil(t, docdb)
	assert.Equal(t, "db1", docdb["DatabaseName"])
	_, hasCollection := docdb["CollectionName"]
	assert.False(t, hasCollection)
}

// ============================================================
// SourceAccessConfiguration — Type and URI shape
// ============================================================

func TestBatch3_SourceAccessConfiguration_TypeAndURI(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sacType string
		uri     string
		name    string
	}{
		{
			name:    "basic_auth",
			sacType: "BASIC_AUTH",
			uri:     "arn:aws:secretsmanager:us-east-1:000000000000:secret:broker-creds",
		},
		{
			name:    "sasl_scram_512",
			sacType: "SASL_SCRAM_512_AUTH",
			uri:     "arn:aws:secretsmanager:us-east-1:000000000000:secret:kafka-scram512",
		},
		{
			name:    "vpc_subnet",
			sacType: "VPC_SUBNET",
			uri:     "subnet-0123456789",
		},
		{
			name:    "vpc_security_group",
			sacType: "VPC_SECURITY_GROUP",
			uri:     "sg-0123456789",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newInMemoryHandler(t)
			createFunctionForTest(t, h, "sac-"+tt.name+"-fn")

			body := `{
				"FunctionName":"sac-` + tt.name + `-fn",
				"EventSourceArn":"arn:aws:kafka:us-east-1:000000000000:cluster/c/x",
				"Topics":["t"],
				"SourceAccessConfigurations":[{"Type":"` + tt.sacType + `","URI":"` + tt.uri + `"}]
			}`
			rec := callInMemoryHandler(t, h, http.MethodPost, "/2015-03-31/event-source-mappings", body)
			require.Equal(t, http.StatusCreated, rec.Code)

			var out map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

			sacs, _ := out["SourceAccessConfigurations"].([]any)
			require.Len(t, sacs, 1)
			sac, _ := sacs[0].(map[string]any)
			assert.Equal(t, tt.sacType, sac["Type"])
			assert.Equal(t, tt.uri, sac["URI"])
		})
	}
}

// TestAuditLambda_ListESM_EventSourceArnFilter verifies ListEventSourceMappings
// filters by EventSourceArn query parameter.
func TestAuditLambda_ListESM_EventSourceArnFilter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		filter    string
		wantCount int
	}{
		{
			name:      "no filter returns all",
			filter:    "",
			wantCount: 2,
		},
		{
			name:      "exact ARN match returns one",
			filter:    "arn:aws:sqs:us-east-1:000000000000:queue-a",
			wantCount: 1,
		},
		{
			name:      "non-matching ARN returns none",
			filter:    "arn:aws:sqs:us-east-1:000000000000:no-such-queue",
			wantCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, bk := newInMemoryHandler(t)
			createFunctionForTest(t, h, "esm-filter-fn")

			// Seed two ESMs with different source ARNs.
			for _, queueSuffix := range []string{"queue-a", "queue-b"} {
				sourceARN := fmt.Sprintf("arn:aws:sqs:us-east-1:000000000000:%s", queueSuffix)
				_, esmErr := bk.CreateEventSourceMapping(&lambda.CreateEventSourceMappingInput{
					EventSourceARN:   sourceARN,
					FunctionName:     "esm-filter-fn",
					StartingPosition: "TRIM_HORIZON",
					Enabled:          true,
				})
				require.NoError(t, esmErr)
			}

			listPath := "/2015-03-31/event-source-mappings/"
			if tc.filter != "" {
				listPath += "?EventSourceArn=" + url.QueryEscape(tc.filter)
			}

			rec := callInMemoryHandler(t, h, http.MethodGet, listPath, "")
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			esms, _ := out["EventSourceMappings"].([]any)
			assert.Len(t, esms, tc.wantCount,
				"EventSourceMappings count mismatch for filter=%q", tc.filter)
		})
	}
}
