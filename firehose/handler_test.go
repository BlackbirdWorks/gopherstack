package firehose_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/firehose"
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
