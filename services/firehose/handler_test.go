package firehose_test

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func newTestFirehoseHandler(t *testing.T) *firehose.Handler {
	t.Helper()

	return firehose.NewHandler(firehose.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doFirehoseRequest(t *testing.T, h *firehose.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "Firehose_20150804."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// createStream is a helper that creates a delivery stream via the API.
func createStream(t *testing.T, h *firehose.Handler, name string, extra ...map[string]any) {
	t.Helper()

	body := map[string]any{"DeliveryStreamName": name}
	if len(extra) > 0 {
		maps.Copy(body, extra[0])
	}

	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", body)
	require.Equal(t, http.StatusOK, rec.Code, "create stream %q: %s", name, rec.Body.String())
}

// auditHandler returns a handler with a mock S3 backend wired in.
func auditHandler(t *testing.T) (*firehose.Handler, *mockS3Storer) {
	t.Helper()

	h := newTestFirehoseHandler(t)
	s3mock := &mockS3Storer{}
	h.Backend.(*firehose.InMemoryBackend).SetS3Backend(s3mock)

	return h, s3mock
}

// auditCreateStream creates a delivery stream via the handler and returns the ARN.
func auditCreateStream(t *testing.T, h *firehose.Handler, name string, extra map[string]any) string {
	t.Helper()

	body := map[string]any{"DeliveryStreamName": name}
	maps.Copy(body, extra)

	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", body)
	require.Equal(t, http.StatusOK, rec.Code, "createStream %q: %s", name, rec.Body.String())

	var out struct {
		DeliveryStreamARN string `json:"DeliveryStreamARN"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out.DeliveryStreamARN
}

// auditDescribe calls DescribeDeliveryStream and returns the raw parsed map.
func auditDescribe(t *testing.T, h *firehose.Handler, name string) map[string]any {
	t.Helper()

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": name})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	return out["DeliveryStreamDescription"].(map[string]any)
}

// auditPut puts a single base64-encoded record via the handler.
func auditPut(t *testing.T, h *firehose.Handler, name string, data string) {
	t.Helper()

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": name,
		"Record":             map[string]any{"Data": base64.StdEncoding.EncodeToString([]byte(data))},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// gunzip decompresses gzip bytes and returns the raw content.
func gunzip(t *testing.T, data []byte) []byte {
	t.Helper()

	r, err := gzip.NewReader(bytes.NewReader(data))
	require.NoError(t, err)
	defer r.Close()

	out, err := io.ReadAll(r)
	require.NoError(t, err)

	return out
}

// singleDestination extracts the sole entry of the Describe response's "Destinations"
// list and returns the nested map found under wireKey (e.g.
// "ExtendedS3DestinationDescription"), matching AWS's DestinationDescription wrapper
// shape where every destination type nests under one "Destinations" array entry.
func singleDestination(t *testing.T, desc map[string]any, wireKey string) map[string]any {
	t.Helper()

	dests, ok := desc["Destinations"].([]any)
	require.True(t, ok, "Destinations must be present")
	require.Len(t, dests, 1)

	entry := dests[0].(map[string]any)
	d, ok := entry[wireKey].(map[string]any)
	require.True(t, ok, "%s must be present in Destinations[0]", wireKey)

	return d
}

func TestFirehoseHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "UnknownAction", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestFirehoseHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "match",
			target: "Firehose_20150804.CreateDeliveryStream",
			want:   true,
		},
		{
			name:   "no_match",
			target: "SimpleWorkflowService.RegisterDomain",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			matcher := h.RouteMatcher()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestFirehoseHandler_ProviderName(t *testing.T) {
	t.Parallel()

	p := &firehose.Provider{}
	assert.Equal(t, "Firehose", p.Name())
}

func TestFirehoseHandler_HandlerName(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	assert.Equal(t, "Firehose", h.Name())
}

func TestFirehoseHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateDeliveryStream")
	assert.Contains(t, ops, "DeleteDeliveryStream")
	assert.Contains(t, ops, "DescribeDeliveryStream")
	assert.Contains(t, ops, "ListDeliveryStreams")
	assert.Contains(t, ops, "PutRecord")
	assert.Contains(t, ops, "PutRecordBatch")
}

func TestFirehoseHandler_GetSupportedOperations_EncryptionOps(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "StartDeliveryStreamEncryption")
	assert.Contains(t, ops, "StopDeliveryStreamEncryption")
}

func TestFirehoseHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestFirehoseHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "with_target",
			target: "Firehose_20150804.CreateDeliveryStream",
			want:   "CreateDeliveryStream",
		},
		{
			name: "no_target",
			want: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.target != "" {
				req.Header.Set("X-Amz-Target", tt.target)
			}
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestFirehoseHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"DeliveryStreamName":"my-stream"}`))
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my-stream", h.ExtractResource(c))
}

func TestFirehoseHandler_ProviderInit(t *testing.T) {
	t.Parallel()

	p := &firehose.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Firehose", svc.Name())
}

// TestProvider_Init_NilCtx verifies Init rejects a nil AppContext.
func TestProvider_Init_NilCtx(t *testing.T) {
	t.Parallel()

	p := &firehose.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrNilAppContext)
}

func TestHandler_Shutdown_FlushesBufferedRecords(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		createStream     bool
		withS3           bool
		putRecords       bool
		wantS3Deliveries int
	}{
		{
			name:             "shutdown on idle backend completes promptly",
			createStream:     false,
			withS3:           false,
			putRecords:       false,
			wantS3Deliveries: 0,
		},
		{
			name:             "shutdown with stream but no buffered records is a no-op",
			createStream:     true,
			withS3:           true,
			putRecords:       false,
			wantS3Deliveries: 0,
		},
		{
			name:             "shutdown flushes buffered records to S3",
			createStream:     true,
			withS3:           true,
			putRecords:       true,
			wantS3Deliveries: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)

			var s3mock *mockS3Storer

			if tt.createStream {
				rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
					"DeliveryStreamName": "test-stream",
					"S3DestinationConfiguration": map[string]any{
						"BucketARN": "arn:aws:s3:::test-bucket",
						"RoleARN":   "arn:aws:iam::000000000000:role/test",
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				if tt.withS3 {
					s3mock = &mockS3Storer{}
					h.Backend.(*firehose.InMemoryBackend).SetS3Backend(s3mock)
				}
			}

			if tt.putRecords {
				rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
					"DeliveryStreamName": "test-stream",
					"Record": map[string]any{
						"Data": "dGVzdC1kYXRh", // base64("test-data")
					},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			// Shutdown should complete without blocking or panicking,
			// and should flush any buffered records.
			h.Shutdown(t.Context())

			if s3mock != nil {
				assert.Len(t, s3mock.calls, tt.wantS3Deliveries,
					"S3 deliveries after Shutdown should match expected count")
			}
		})
	}
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "s1"})
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "s2"})

	h.Reset()

	rec := doFirehoseRequest(t, h, "ListDeliveryStreams", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"DeliveryStreamNames":[]`)
}

func TestHandler_OpsLen(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	assert.Equal(t, 12, firehose.HandlerOpsLen(h))
}

// --- Error response __type field ---

// Real AWS Firehose returns __type in every error response so SDK clients can
// deserialize errors into typed structs.  The handler must include __type for
// 400-class errors, not only 404s.

func TestErrorResponse_ResourceInUseException_HasType(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "dup-stream")

	rec := doFirehoseRequest(t, h, "CreateDeliveryStream",
		map[string]any{"DeliveryStreamName": "dup-stream"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceInUseException", body["__type"],
		"duplicate-stream error must carry __type=ResourceInUseException")
}

func TestErrorResponse_InvalidArgumentException_HasType(t *testing.T) {
	t.Parallel()

	longKey := make([]byte, 129)
	for i := range longKey {
		longKey[i] = 'x'
	}

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "empty_record_data",
			action: "PutRecord",
			body: map[string]any{
				"DeliveryStreamName": "invalid-stream",
				"Record":             map[string]any{"Data": ""},
			},
		},
		{
			name:   "tag_key_too_long",
			action: "CreateDeliveryStream",
			body: map[string]any{
				"DeliveryStreamName": "tag-err-stream",
				"Tags": []map[string]string{
					{"Key": string(longKey), "Value": "v"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)
			// Pre-create the stream for PutRecord tests.
			if tt.action == "PutRecord" {
				createStream(t, h, "invalid-stream")
			}

			rec := doFirehoseRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "InvalidArgumentException", body["__type"],
				"validation error must carry __type=InvalidArgumentException; got: %v", body)
		})
	}
}

func TestErrorResponse_ResourceNotFoundException_HasType(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "no-such-stream"})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"],
		"not-found error must carry __type=ResourceNotFoundException")
}

func TestErrorResponse_UnknownOperation_HasType(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "NoSuchAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "UnknownOperationException", body["__type"],
		"unknown operation must carry __type=UnknownOperationException")
}
