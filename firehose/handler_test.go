package firehose_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/firehose"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

func newTestFirehoseHandler(t *testing.T) *firehose.Handler {
	t.Helper()

	return firehose.NewHandler(firehose.NewInMemoryBackend("000000000000", "us-east-1"), slog.Default())
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

func TestFirehose_Handler_CreateDeliveryStream(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)

	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{
		"DeliveryStreamName": "my-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["DeliveryStreamARN"], "arn:aws:firehose:")
}

func TestFirehose_Handler_DeleteDeliveryStream(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})

	rec := doFirehoseRequest(t, h, "DeleteDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestFirehose_Handler_DescribeDeliveryStream(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "DeliveryStreamDescription")
}

func TestFirehose_Handler_ListDeliveryStreams(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "s1"})
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "s2"})

	rec := doFirehoseRequest(t, h, "ListDeliveryStreams", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp, "DeliveryStreamNames")
}

func TestFirehose_Handler_PutRecord(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})

	data := base64.StdEncoding.EncodeToString([]byte("hello world"))
	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "my-stream",
		"Record":             map[string]string{"Data": data},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["RecordId"])
}

func TestFirehose_Handler_PutRecordBatch(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})

	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "my-stream",
		"Records": []map[string]string{
			{"Data": base64.StdEncoding.EncodeToString([]byte("rec1"))},
			{"Data": base64.StdEncoding.EncodeToString([]byte("rec2"))},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestFirehose_Handler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)

	rec := doFirehoseRequest(t, h, "UnknownAction", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestFirehose_Handler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "Firehose_20150804.CreateDeliveryStream")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.True(t, matcher(c))
}

func TestFirehose_Provider(t *testing.T) {
	t.Parallel()

	p := &firehose.Provider{}
	assert.Equal(t, "Firehose", p.Name())
}

func TestFirehose_Handler_Name(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	assert.Equal(t, "Firehose", h.Name())
}

func TestFirehose_Handler_GetSupportedOperations(t *testing.T) {
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

func TestFirehose_Handler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	assert.Equal(t, 100, h.MatchPriority())
}

func TestFirehose_Handler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "Firehose_20150804.CreateDeliveryStream")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "CreateDeliveryStream", h.ExtractOperation(c))

	// No target → "Unknown"
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.Equal(t, "Unknown", h.ExtractOperation(c2))
}

func TestFirehose_Handler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"DeliveryStreamName":"my-stream"}`))
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "my-stream", h.ExtractResource(c))
}

func TestFirehose_Handler_RouteMatcher_NoMatch(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "SimpleWorkflowService.RegisterDomain")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.False(t, matcher(c))
}

func TestFirehose_Handler_CreateDeliveryStream_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})

	rec := doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestFirehose_Handler_DeleteDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)

	rec := doFirehoseRequest(t, h, "DeleteDeliveryStream", map[string]any{"DeliveryStreamName": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFirehose_Handler_DescribeDeliveryStream_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)

	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream", map[string]any{"DeliveryStreamName": "nonexistent"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFirehose_Handler_PutRecord_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)

	data := base64.StdEncoding.EncodeToString([]byte("hello"))
	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "nonexistent",
		"Record":             map[string]string{"Data": data},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFirehose_Handler_PutRecord_RawData(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})

	// Non-base64 data falls back to raw bytes
	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "my-stream",
		"Record":             map[string]string{"Data": "not-base64!@#"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestFirehose_Handler_PutRecordBatch_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)

	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "nonexistent",
		"Records":            []map[string]string{{"Data": base64.StdEncoding.EncodeToString([]byte("a"))}},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestFirehose_Handler_PutRecordBatch_RawData(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})

	// Non-base64 data falls back to raw bytes
	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "my-stream",
		"Records":            []map[string]string{{"Data": "not-base64!@#"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestFirehose_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &firehose.Provider{}
	ctx := &service.AppContext{Logger: slog.Default()}
	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
	assert.Equal(t, "Firehose", svc.Name())
}
