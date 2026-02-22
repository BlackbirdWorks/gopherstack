package sqs_test

import (
	"encoding/xml"
	"errors"
	"fmt"
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
	"github.com/blackbirdworks/gopherstack/pkgs/service"
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

	// Match: root path + form-encoded (CreateQueue, ListQueues)
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, matcher(c))

	// Match: /000000000000/queue path + form-encoded (SendMessage, etc.)
	req3 := httptest.NewRequest(http.MethodPost, "/000000000000/my-queue", nil)
	req3.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c3 := e.NewContext(req3, httptest.NewRecorder())
	assert.True(t, matcher(c3))

	// No match: wrong Content-Type
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	req2.Header.Set("Content-Type", "application/json")
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.False(t, matcher(c2))

	// No match: dashboard HTMX form (form-encoded but wrong path)
	req4 := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create", nil)
	req4.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c4 := e.NewContext(req4, httptest.NewRecorder())
	assert.False(t, matcher(c4))
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

func TestHandlerName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "SQS", h.Name())
	assert.NotEmpty(t, h.GetSupportedOperations())
	assert.Equal(t, 75, h.MatchPriority())
}

func TestHandlerDeleteQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("del-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doRequest(t, h, url.Values{
		"Action":   {"DeleteQueue"},
		"QueueUrl": {createResp.CreateQueueResult.QueueURL},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.DeleteQueueResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
}

func TestHandlerDeleteQueueNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, url.Values{
		"Action":   {"DeleteQueue"},
		"QueueUrl": {"http://localhost/000000000000/noqueue"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerGetQueueAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("attr-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doRequest(t, h, url.Values{
		"Action":          {"GetQueueAttributes"},
		"QueueUrl":        {createResp.CreateQueueResult.QueueURL},
		"AttributeName.1": {"All"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.GetQueueAttributesResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.GetQueueAttributesResult.Attributes)
}

func TestHandlerGetQueueAttributesNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, url.Values{
		"Action":   {"GetQueueAttributes"},
		"QueueUrl": {"http://localhost/000000000000/noqueue"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerSetQueueAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("set-attr-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doRequest(t, h, url.Values{
		"Action":            {"SetQueueAttributes"},
		"QueueUrl":          {createResp.CreateQueueResult.QueueURL},
		"Attribute.1.Name":  {"VisibilityTimeout"},
		"Attribute.1.Value": {"60"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.SetQueueAttributesResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
}

func TestHandlerSetQueueAttributesNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, url.Values{
		"Action":            {"SetQueueAttributes"},
		"QueueUrl":          {"http://localhost/000000000000/noqueue"},
		"Attribute.1.Name":  {"VisibilityTimeout"},
		"Attribute.1.Value": {"60"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerChangeMessageVisibility(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("vis-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	qURL := createResp.CreateQueueResult.QueueURL

	doRequest(t, h, url.Values{
		"Action":      {"SendMessage"},
		"QueueUrl":    {qURL},
		"MessageBody": {"hello"},
	})

	recvRec := doRequest(t, h, url.Values{
		"Action":   {"ReceiveMessage"},
		"QueueUrl": {qURL},
	})

	var recvResp sqs.ReceiveMessageResponse
	require.NoError(t, xml.Unmarshal(recvRec.Body.Bytes(), &recvResp))
	require.Len(t, recvResp.ReceiveMessageResult.Messages, 1)

	receipt := recvResp.ReceiveMessageResult.Messages[0].ReceiptHandle

	rec := doRequest(t, h, url.Values{
		"Action":            {"ChangeMessageVisibility"},
		"QueueUrl":          {qURL},
		"ReceiptHandle":     {receipt},
		"VisibilityTimeout": {"10"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.ChangeMessageVisibilityResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.ResponseMetadata.RequestID)
}

func TestHandlerChangeMessageVisibilityNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("vis-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doRequest(t, h, url.Values{
		"Action":            {"ChangeMessageVisibility"},
		"QueueUrl":          {createResp.CreateQueueResult.QueueURL},
		"ReceiptHandle":     {"invalid-receipt"},
		"VisibilityTimeout": {"10"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerSendMessageBatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("batch-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	qURL := createResp.CreateQueueResult.QueueURL

	rec := doRequest(t, h, url.Values{
		"Action":                            {"SendMessageBatch"},
		"QueueUrl":                          {qURL},
		"SendMessageBatchRequestEntry.1.Id": {"msg1"},
		"SendMessageBatchRequestEntry.1.MessageBody": {"hello1"},
		"SendMessageBatchRequestEntry.2.Id":          {"msg2"},
		"SendMessageBatchRequestEntry.2.MessageBody": {"hello2"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.SendMessageBatchResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.SendMessageBatchResult.Successful, 2)
}

func TestHandlerSendMessageBatchNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, url.Values{
		"Action":                            {"SendMessageBatch"},
		"QueueUrl":                          {"http://localhost/000000000000/noqueue"},
		"SendMessageBatchRequestEntry.1.Id": {"msg1"},
		"SendMessageBatchRequestEntry.1.MessageBody": {"hello"},
	})
	// Batch returns 200 with failures for individual entries on non-existent queue
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.SendMessageBatchResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.SendMessageBatchResult.Failed, 1)
}

func TestHandlerDeleteMessageBatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("del-batch-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	qURL := createResp.CreateQueueResult.QueueURL

	doRequest(t, h, url.Values{
		"Action":      {"SendMessage"},
		"QueueUrl":    {qURL},
		"MessageBody": {"hello"},
	})

	recvRec := doRequest(t, h, url.Values{
		"Action":   {"ReceiveMessage"},
		"QueueUrl": {qURL},
	})

	var recvResp sqs.ReceiveMessageResponse
	require.NoError(t, xml.Unmarshal(recvRec.Body.Bytes(), &recvResp))
	require.Len(t, recvResp.ReceiveMessageResult.Messages, 1)

	receipt := recvResp.ReceiveMessageResult.Messages[0].ReceiptHandle

	rec := doRequest(t, h, url.Values{
		"Action":                              {"DeleteMessageBatch"},
		"QueueUrl":                            {qURL},
		"DeleteMessageBatchRequestEntry.1.Id": {"entry1"},
		"DeleteMessageBatchRequestEntry.1.ReceiptHandle": {receipt},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.DeleteMessageBatchResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.DeleteMessageBatchResult.Successful, 1)
}

func TestHandlerDeleteMessageBatchNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, url.Values{
		"Action":                              {"DeleteMessageBatch"},
		"QueueUrl":                            {"http://localhost/000000000000/noqueue"},
		"DeleteMessageBatchRequestEntry.1.Id": {"entry1"},
		"DeleteMessageBatchRequestEntry.1.ReceiptHandle": {"some-receipt"},
	})
	// Batch returns 200 with failures for individual entries on non-existent queue
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.DeleteMessageBatchResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.DeleteMessageBatchResult.Failed, 1)
}

func TestHandlerDeleteMessageBatchFailedEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("del-fail-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	qURL := createResp.CreateQueueResult.QueueURL

	rec := doRequest(t, h, url.Values{
		"Action":                              {"DeleteMessageBatch"},
		"QueueUrl":                            {qURL},
		"DeleteMessageBatchRequestEntry.1.Id": {"entry1"},
		"DeleteMessageBatchRequestEntry.1.ReceiptHandle": {"invalid-receipt"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.DeleteMessageBatchResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.DeleteMessageBatchResult.Failed, 1)
}

func TestHandlerReceiveMessageWithVisibilityTimeout(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("vt-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	qURL := createResp.CreateQueueResult.QueueURL

	doRequest(t, h, url.Values{
		"Action":      {"SendMessage"},
		"QueueUrl":    {qURL},
		"MessageBody": {"hello"},
	})

	rec := doRequest(t, h, url.Values{
		"Action":            {"ReceiveMessage"},
		"QueueUrl":          {qURL},
		"VisibilityTimeout": {"30"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.ReceiveMessageResponse
	err := xml.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.ReceiveMessageResult.Messages, 1)
}

func TestHandlerPurgeQueueNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, url.Values{
		"Action":   {"PurgeQueue"},
		"QueueUrl": {"http://localhost/000000000000/noqueue"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerDeleteMessageNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("del-msg-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doRequest(t, h, url.Values{
		"Action":        {"DeleteMessage"},
		"QueueUrl":      {createResp.CreateQueueResult.QueueURL},
		"ReceiptHandle": {"invalid-receipt"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerGetQueueURLNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, url.Values{
		"Action":    {"GetQueueUrl"},
		"QueueName": {"nonexistent-queue"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestProviderNameAndInit(t *testing.T) {
	t.Parallel()

	p := &sqs.Provider{}
	assert.Equal(t, "SQS", p.Name())

	log := logger.NewLogger(slog.LevelDebug)
	appCtx := &service.AppContext{Logger: log}
	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

func TestHandlerSendMessageBatchEmptyEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("empty-batch-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	// No batch entries — should return error via writeError.
	rec := doRequest(t, h, url.Values{
		"Action":   {"SendMessageBatch"},
		"QueueUrl": {createResp.CreateQueueResult.QueueURL},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sqs.XMLErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "AWS.SimpleQueueService.EmptyBatchRequest", errResp.Error.Code)
}

func TestHandlerDeleteMessageBatchEmptyEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("empty-del-batch-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doRequest(t, h, url.Values{
		"Action":   {"DeleteMessageBatch"},
		"QueueUrl": {createResp.CreateQueueResult.QueueURL},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sqs.XMLErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "AWS.SimpleQueueService.EmptyBatchRequest", errResp.Error.Code)
}

func TestHandlerSendMessageBatchTooManyEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, formCreateQueue("toomany-batch-queue"))

	var createResp sqs.CreateQueueResponse
	require.NoError(t, xml.Unmarshal(createRec.Body.Bytes(), &createResp))

	// parseSendBatchEntries caps at maxBatchSize (10), so we can only test up to 10 entries
	// which succeed. The TooManyEntriesInBatch error is only reachable via the backend
	// directly (tested in backend_test.go). Here we verify 10 entries succeed fine.
	form := url.Values{
		"Action":   {"SendMessageBatch"},
		"QueueUrl": {createResp.CreateQueueResult.QueueURL},
	}
	for i := 1; i <= 10; i++ {
		form.Set(fmt.Sprintf("SendMessageBatchRequestEntry.%d.Id", i), fmt.Sprintf("msg%d", i))
		form.Set(fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageBody", i), "body")
	}

	rec := doRequest(t, h, form)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp sqs.SendMessageBatchResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.SendMessageBatchResult.Successful, 10)
}

func TestHandlerExtractResourceNoQueueURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	body := url.Values{"Action": {"SendMessage"}}.Encode() // no QueueUrl
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Empty(t, h.ExtractResource(c))
}

func TestHandlerInvalidBodyParsing(t *testing.T) {
	t.Parallel()

	// An invalid URL-encoded body (with percent-encoding errors) causes ParseQuery to fail.
	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("%zz"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// errorBackend is a StorageBackend that always returns an error for all operations.
type errorBackend struct {
	err error
}

func (e *errorBackend) CreateQueue(_ *sqs.CreateQueueInput) (*sqs.CreateQueueOutput, error) {
	return nil, e.err
}

func (e *errorBackend) DeleteQueue(_ *sqs.DeleteQueueInput) error { return e.err }

func (e *errorBackend) ListQueues(_ *sqs.ListQueuesInput) (*sqs.ListQueuesOutput, error) {
	return nil, e.err
}

func (e *errorBackend) GetQueueURL(_ *sqs.GetQueueURLInput) (*sqs.GetQueueURLOutput, error) {
	return nil, e.err
}

func (e *errorBackend) GetQueueAttributes(
	_ *sqs.GetQueueAttributesInput,
) (*sqs.GetQueueAttributesOutput, error) {
	return nil, e.err
}

func (e *errorBackend) SetQueueAttributes(_ *sqs.SetQueueAttributesInput) error { return e.err }

func (e *errorBackend) SendMessage(_ *sqs.SendMessageInput) (*sqs.SendMessageOutput, error) {
	return nil, e.err
}

func (e *errorBackend) ReceiveMessage(
	_ *sqs.ReceiveMessageInput,
) (*sqs.ReceiveMessageOutput, error) {
	return nil, e.err
}

func (e *errorBackend) DeleteMessage(_ *sqs.DeleteMessageInput) error { return e.err }

func (e *errorBackend) ChangeMessageVisibility(
	_ *sqs.ChangeMessageVisibilityInput,
) error {
	return e.err
}

func (e *errorBackend) SendMessageBatch(
	_ *sqs.SendMessageBatchInput,
) (*sqs.SendMessageBatchOutput, error) {
	return nil, e.err
}

func (e *errorBackend) DeleteMessageBatch(
	_ *sqs.DeleteMessageBatchInput,
) (*sqs.DeleteMessageBatchOutput, error) {
	return nil, e.err
}

func (e *errorBackend) PurgeQueue(_ *sqs.PurgeQueueInput) error { return e.err }

func (e *errorBackend) ListAll() []*sqs.Queue { return nil }

func newErrorHandler(t *testing.T, err error) *sqs.Handler {
	t.Helper()

	log := logger.NewLogger(slog.LevelDebug)

	return sqs.NewHandler(&errorBackend{err: err}, log)
}

func TestHandlerListQueuesError(t *testing.T) {
	t.Parallel()

	h := newErrorHandler(t, sqs.ErrQueueNotFound)
	rec := doRequest(t, h, url.Values{
		"Action": {"ListQueues"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sqs.XMLErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "AWS.SimpleQueueService.NonExistentQueue", errResp.Error.Code)
}

func TestHandlerErrorDetailsInvalidAttribute(t *testing.T) {
	t.Parallel()

	h := newErrorHandler(t, sqs.ErrInvalidAttribute)
	rec := doRequest(t, h, url.Values{
		"Action":   {"SetQueueAttributes"},
		"QueueUrl": {"http://localhost/000000000000/q"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sqs.XMLErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InvalidAttributeValue", errResp.Error.Code)
}

func TestHandlerErrorDetailsTooManyEntriesInBatch(t *testing.T) {
	t.Parallel()

	h := newErrorHandler(t, sqs.ErrTooManyEntriesInBatch)
	rec := doRequest(t, h, url.Values{
		"Action":                                    {"SendMessageBatch"},
		"QueueUrl":                                  {"http://localhost/000000000000/q"},
		"SendMessageBatchRequestEntry.1.Id":         {"1"},
		"SendMessageBatchRequestEntry.1.MessageBody": {"body"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp sqs.XMLErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "AWS.SimpleQueueService.TooManyEntriesInBatchRequest", errResp.Error.Code)
}

func TestHandlerErrorDetailsInternalError(t *testing.T) {
	t.Parallel()

	// Use a non-sentinel error to trigger the default internal error case.
	h := newErrorHandler(t, errors.New("unexpected internal error"))
	rec := doRequest(t, h, url.Values{
		"Action":   {"PurgeQueue"},
		"QueueUrl": {"http://localhost/000000000000/q"},
	})
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	var errResp sqs.XMLErrorResponse
	require.NoError(t, xml.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "InternalError", errResp.Error.Code)
}

func TestHandlerExtractOperationInvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	// Invalid URL-encoded body causes ParseQuery to fail → unknownOperation.
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("%zz"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "Unknown", h.ExtractOperation(c))
}

func TestHandlerExtractResourceInvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("%zz"))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Empty(t, h.ExtractResource(c))
}

func TestHandlerQueueNameFromURLEdgeCases(t *testing.T) {
	t.Parallel()

	h := newErrorHandler(t, sqs.ErrQueueNotFound)

	// QueueUrl with an empty URL - queueNameFromURL splits on "/" and returns last part.
	rec := doRequest(t, h, url.Values{
		"Action":   {"DeleteQueue"},
		"QueueUrl": {""},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}
