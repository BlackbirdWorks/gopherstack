package sqs_test

import (
	"encoding/xml"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/sqs"
)

func newTestHandler(t *testing.T) *sqs.Handler {
	t.Helper()

	log := logger.NewLogger(slog.LevelDebug)
	backend := sqs.NewInMemoryBackend()

	return sqs.NewHandler(backend, log)
}

func doRequest(t *testing.T, h *sqs.Handler, form url.Values) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body := form.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func formCreateQueue(name string) url.Values {
	return url.Values{
		"Action":    {"CreateQueue"},
		"QueueName": {name},
	}
}

func TestHandlerCreateQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, formCreateQueue("test-queue"))

	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.CreateQueueResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Contains(t, resp.CreateQueueResult.QueueURL, "test-queue")
	assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
}

func TestHandlerCreateQueueDuplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, formCreateQueue("test-queue"))
	rec := doRequest(t, h, formCreateQueue("test-queue"))

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sqs.XMLErrorResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "QueueAlreadyExists", errResp.Error.Code)
}

func TestHandlerListQueues(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, formCreateQueue("queue-a"))
	doRequest(t, h, formCreateQueue("queue-b"))

	rec := doRequest(t, h, url.Values{"Action": {"ListQueues"}})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.ListQueuesResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.ListQueuesResult.QueueURLs, 2)
}

func TestHandlerListQueuesWithPrefix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, formCreateQueue("alpha-queue"))
	doRequest(t, h, formCreateQueue("beta-queue"))

	rec := doRequest(t, h, url.Values{
		"Action":          {"ListQueues"},
		"QueueNamePrefix": {"alpha"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.ListQueuesResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.ListQueuesResult.QueueURLs, 1)
}

func TestHandlerSendMessage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("my-queue"))
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	queueURLStr := createResp.CreateQueueResult.QueueURL

	rec := doRequest(t, h, url.Values{
		"Action":      {"SendMessage"},
		"QueueUrl":    {queueURLStr},
		"MessageBody": {"hello from handler"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.SendMessageResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.SendMessageResult.MessageID)
	assert.NotEmpty(t, resp.SendMessageResult.MD5OfMessageBody)
}

func TestHandlerReceiveMessage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("my-queue"))
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	queueURLStr := createResp.CreateQueueResult.QueueURL

	doRequest(t, h, url.Values{
		"Action":      {"SendMessage"},
		"QueueUrl":    {queueURLStr},
		"MessageBody": {"hello"},
	})

	rec := doRequest(t, h, url.Values{
		"Action":              {"ReceiveMessage"},
		"QueueUrl":            {queueURLStr},
		"MaxNumberOfMessages": {"1"},
		"WaitTimeSeconds":     {"0"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.ReceiveMessageResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	require.Len(t, resp.ReceiveMessageResult.Messages, 1)
	assert.Equal(t, "hello", resp.ReceiveMessageResult.Messages[0].Body)
}

func TestHandlerDeleteMessage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("my-queue"))
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	queueURLStr := createResp.CreateQueueResult.QueueURL

	doRequest(t, h, url.Values{
		"Action":      {"SendMessage"},
		"QueueUrl":    {queueURLStr},
		"MessageBody": {"hello"},
	})

	recvRec := doRequest(t, h, url.Values{
		"Action":              {"ReceiveMessage"},
		"QueueUrl":            {queueURLStr},
		"MaxNumberOfMessages": {"1"},
	})

	var recvResp sqs.ReceiveMessageResponse
	require.NoError(t, xml.Unmarshal(recvRec.Body.Bytes(), &recvResp))
	require.Len(t, recvResp.ReceiveMessageResult.Messages, 1)

	receipt := recvResp.ReceiveMessageResult.Messages[0].ReceiptHandle

	rec := doRequest(t, h, url.Values{
		"Action":        {"DeleteMessage"},
		"QueueUrl":      {queueURLStr},
		"ReceiptHandle": {receipt},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.DeleteMessageResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
}

func TestHandlerMissingAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, url.Values{"QueueName": {"test"}})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sqs.XMLErrorResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "InvalidAction", errResp.Error.Code)
}

func TestHandlerUnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, url.Values{"Action": {"NonExistentAction"}})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sqs.XMLErrorResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "InvalidAction", errResp.Error.Code)
}

func TestHandlerQueueNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, url.Values{
		"Action":      {"SendMessage"},
		"QueueUrl":    {"http://localhost/000000000000/nonexistent"},
		"MessageBody": {"hello"},
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sqs.XMLErrorResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "AWS.SimpleQueueService.NonExistentQueue", errResp.Error.Code)
}

func TestHandlerGetQueueURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, formCreateQueue("my-queue"))

	rec := doRequest(t, h, url.Values{
		"Action":    {"GetQueueUrl"},
		"QueueName": {"my-queue"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.GetQueueURLResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Contains(t, resp.GetQueueURLResult.QueueURL, "my-queue")
}

func TestHandlerPurgeQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("my-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	queueURLStr := createResp.CreateQueueResult.QueueURL

	doRequest(t, h, url.Values{
		"Action":      {"SendMessage"},
		"QueueUrl":    {queueURLStr},
		"MessageBody": {"hello"},
	})

	rec := doRequest(t, h, url.Values{
		"Action":   {"PurgeQueue"},
		"QueueUrl": {queueURLStr},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.PurgeQueueResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
}

func TestHandlerRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.True(t, matcher(c))

	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	req2.Header.Set("Content-Type", "application/json")
	c2 := e.NewContext(req2, httptest.NewRecorder())

	assert.False(t, matcher(c2))
}

func TestHandlerExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	body := url.Values{"Action": {"SendMessage"}, "QueueUrl": {"http://x/000000000000/q"}}.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "SendMessage", h.ExtractOperation(c))
	assert.Equal(t, "q", h.ExtractResource(c))
}
