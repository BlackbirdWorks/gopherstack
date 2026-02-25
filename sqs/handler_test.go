package sqs_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/sqs"
)

// errInternalTest is a sentinel used to exercise the default InternalError branch in errorDetails.
var errInternalTest = errors.New("unexpected internal error")

// jsonErr is a convenience struct for parsing JSON error responses.
type jsonErr struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

func newTestHandler(t *testing.T) *sqs.Handler {
	t.Helper()

	log := logger.NewLogger(slog.LevelDebug)
	backend := sqs.NewInMemoryBackend()

	return sqs.NewHandler(backend, log)
}

// doRequest sends a JSON request to the handler with the given X-Amz-Target action.
// Pass action="" to omit the X-Amz-Target header (tests missing action handling).
func doRequest(t *testing.T, h *sqs.Handler, action string, body any) *httptest.ResponseRecorder {
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
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")

	if action != "" {
		req.Header.Set("X-Amz-Target", "AmazonSQS."+action)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func doCreateQueue(t *testing.T, h *sqs.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, "CreateQueue", map[string]any{"QueueName": name})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		QueueURL string `json:"QueueUrl"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	return resp.QueueURL
}

func TestHandlerCreateQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "CreateQueue", map[string]any{"QueueName": "test-queue"})

	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		QueueURL string `json:"QueueUrl"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Contains(t, resp.QueueURL, "test-queue")
}

func TestHandlerCreateQueueDuplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateQueue", map[string]any{"QueueName": "test-queue"})
	rec := doRequest(t, h, "CreateQueue", map[string]any{"QueueName": "test-queue"})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp jsonErr
	err := json.Unmarshal(rec.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "com.amazonaws.sqs#QueueNameExists", errResp.Type)
}

func TestHandlerListQueues(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doCreateQueue(t, h, "queue-a")
	doCreateQueue(t, h, "queue-b")

	rec := doRequest(t, h, "ListQueues", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		QueueURLs []string `json:"QueueUrls"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.QueueURLs, 2)
}

func TestHandlerListQueuesWithPrefix(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doCreateQueue(t, h, "alpha-queue")
	doCreateQueue(t, h, "beta-queue")

	rec := doRequest(t, h, "ListQueues", map[string]any{"QueueNamePrefix": "alpha"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		QueueURLs []string `json:"QueueUrls"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.QueueURLs, 1)
}

func TestHandlerSendMessage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "my-queue")

	rec := doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello from handler",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		MessageID        string `json:"MessageId"`
		MD5OfMessageBody string `json:"MD5OfMessageBody"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.MessageID)
	assert.NotEmpty(t, resp.MD5OfMessageBody)
}

func TestHandlerReceiveMessage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "my-queue")

	doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello",
	})

	rec := doRequest(t, h, "ReceiveMessage", map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
		"WaitTimeSeconds":     0,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Messages []struct {
			Body string `json:"Body"`
		} `json:"Messages"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	require.Len(t, resp.Messages, 1)
	assert.Equal(t, "hello", resp.Messages[0].Body)
}

func TestHandlerDeleteMessage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "my-queue")

	doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello",
	})

	recvRec := doRequest(t, h, "ReceiveMessage", map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
	})

	var recvResp struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}

	require.NoError(t, json.Unmarshal(recvRec.Body.Bytes(), &recvResp))
	require.Len(t, recvResp.Messages, 1)

	receipt := recvResp.Messages[0].ReceiptHandle

	rec := doRequest(t, h, "DeleteMessage", map[string]any{
		"QueueUrl":      queueURL,
		"ReceiptHandle": receipt,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerMissingAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "", map[string]any{"QueueName": "test"})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp jsonErr
	err := json.Unmarshal(rec.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "com.amazonaws.sqs#InvalidAction", errResp.Type)
}

func TestHandlerUnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "NonExistentAction", map[string]any{})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp jsonErr
	err := json.Unmarshal(rec.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "com.amazonaws.sqs#InvalidAction", errResp.Type)
}

func TestHandlerQueueNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    "http://localhost/000000000000/nonexistent",
		"MessageBody": "hello",
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp jsonErr
	err := json.Unmarshal(rec.Body.Bytes(), &errResp)
	require.NoError(t, err)
	assert.Equal(t, "AWS.SimpleQueueService.NonExistentQueue", errResp.Type)
}

func TestHandlerGetQueueURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doCreateQueue(t, h, "my-queue")

	rec := doRequest(t, h, "GetQueueUrl", map[string]any{"QueueName": "my-queue"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		QueueURL string `json:"QueueUrl"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Contains(t, resp.QueueURL, "my-queue")
}

func TestHandlerPurgeQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "my-queue")

	doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello",
	})

	rec := doRequest(t, h, "PurgeQueue", map[string]any{"QueueUrl": queueURL})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerRouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	e := echo.New()

	// Match: root path + AmazonSQS. target
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, matcher(c))

	// Match: /000000000000/queue path + AmazonSQS. target
	req3 := httptest.NewRequest(http.MethodPost, "/000000000000/my-queue", nil)
	req3.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
	c3 := e.NewContext(req3, httptest.NewRecorder())
	assert.True(t, matcher(c3))

	// No match: missing X-Amz-Target
	req2 := httptest.NewRequest(http.MethodPost, "/", nil)
	c2 := e.NewContext(req2, httptest.NewRecorder())
	assert.False(t, matcher(c2))

	// No match: wrong path even with AmazonSQS. target
	req4 := httptest.NewRequest(http.MethodPost, "/dashboard/sqs/create", nil)
	req4.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
	c4 := e.NewContext(req4, httptest.NewRecorder())
	assert.False(t, matcher(c4))
}

func TestHandlerExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	body, _ := json.Marshal(map[string]any{"QueueUrl": "http://x/000000000000/q"})
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
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
	queueURL := doCreateQueue(t, h, "del-queue")

	rec := doRequest(t, h, "DeleteQueue", map[string]any{"QueueUrl": queueURL})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteQueueNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteQueue", map[string]any{
		"QueueUrl": "http://localhost/000000000000/noqueue",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerGetQueueAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "attr-queue")

	rec := doRequest(t, h, "GetQueueAttributes", map[string]any{
		"QueueUrl":       queueURL,
		"AttributeNames": []string{"All"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Attributes map[string]string `json:"Attributes"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.NotEmpty(t, resp.Attributes)
}

func TestHandlerGetQueueAttributesNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetQueueAttributes", map[string]any{
		"QueueUrl": "http://localhost/000000000000/noqueue",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerSetQueueAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "set-attr-queue")

	rec := doRequest(t, h, "SetQueueAttributes", map[string]any{
		"QueueUrl":   queueURL,
		"Attributes": map[string]string{"VisibilityTimeout": "60"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerSetQueueAttributesNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "SetQueueAttributes", map[string]any{
		"QueueUrl":   "http://localhost/000000000000/noqueue",
		"Attributes": map[string]string{"VisibilityTimeout": "60"},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerChangeMessageVisibility(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "vis-queue")

	doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello",
	})

	recvRec := doRequest(t, h, "ReceiveMessage", map[string]any{"QueueUrl": queueURL})

	var recvResp struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}

	require.NoError(t, json.Unmarshal(recvRec.Body.Bytes(), &recvResp))
	require.Len(t, recvResp.Messages, 1)

	receipt := recvResp.Messages[0].ReceiptHandle

	rec := doRequest(t, h, "ChangeMessageVisibility", map[string]any{
		"QueueUrl":          queueURL,
		"ReceiptHandle":     receipt,
		"VisibilityTimeout": 10,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerChangeMessageVisibilityNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "vis-queue")

	rec := doRequest(t, h, "ChangeMessageVisibility", map[string]any{
		"QueueUrl":          queueURL,
		"ReceiptHandle":     "invalid-receipt",
		"VisibilityTimeout": 10,
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerSendMessageBatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "batch-queue")

	rec := doRequest(t, h, "SendMessageBatch", map[string]any{
		"QueueUrl": queueURL,
		"Entries": []map[string]any{
			{"Id": "msg1", "MessageBody": "hello1"},
			{"Id": "msg2", "MessageBody": "hello2"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Successful []struct {
			ID string `json:"Id"`
		} `json:"Successful"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.Successful, 2)
}

func TestHandlerSendMessageBatchNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "SendMessageBatch", map[string]any{
		"QueueUrl": "http://localhost/000000000000/noqueue",
		"Entries":  []map[string]any{{"Id": "msg1", "MessageBody": "hello"}},
	})
	// Batch returns 200 with failures for individual entries on non-existent queue
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Failed []struct {
			ID string `json:"Id"`
		} `json:"Failed"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.Failed, 1)
}

func TestHandlerDeleteMessageBatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "del-batch-queue")

	doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello",
	})

	recvRec := doRequest(t, h, "ReceiveMessage", map[string]any{"QueueUrl": queueURL})

	var recvResp struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}

	require.NoError(t, json.Unmarshal(recvRec.Body.Bytes(), &recvResp))
	require.Len(t, recvResp.Messages, 1)

	receipt := recvResp.Messages[0].ReceiptHandle

	rec := doRequest(t, h, "DeleteMessageBatch", map[string]any{
		"QueueUrl": queueURL,
		"Entries":  []map[string]any{{"Id": "entry1", "ReceiptHandle": receipt}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Successful []struct {
			ID string `json:"Id"`
		} `json:"Successful"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.Successful, 1)
}

func TestHandlerDeleteMessageBatchNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "DeleteMessageBatch", map[string]any{
		"QueueUrl": "http://localhost/000000000000/noqueue",
		"Entries":  []map[string]any{{"Id": "entry1", "ReceiptHandle": "some-receipt"}},
	})
	// Batch returns 200 with failures for individual entries on non-existent queue
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Failed []struct {
			ID string `json:"Id"`
		} `json:"Failed"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.Failed, 1)
}

func TestHandlerDeleteMessageBatchFailedEntry(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "del-fail-queue")

	rec := doRequest(t, h, "DeleteMessageBatch", map[string]any{
		"QueueUrl": queueURL,
		"Entries":  []map[string]any{{"Id": "entry1", "ReceiptHandle": "invalid-receipt"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Failed []struct {
			ID string `json:"Id"`
		} `json:"Failed"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.Failed, 1)
}

func TestHandlerReceiveMessageWithVisibilityTimeout(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "vt-queue")

	doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello",
	})

	vt := 30
	rec := doRequest(t, h, "ReceiveMessage", map[string]any{
		"QueueUrl":          queueURL,
		"VisibilityTimeout": vt,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Messages []struct {
			Body string `json:"Body"`
		} `json:"Messages"`
	}

	err := json.Unmarshal(rec.Body.Bytes(), &resp)
	require.NoError(t, err)
	assert.Len(t, resp.Messages, 1)
}

func TestHandlerPurgeQueueNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "PurgeQueue", map[string]any{
		"QueueUrl": "http://localhost/000000000000/noqueue",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerDeleteMessageNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "del-msg-queue")

	rec := doRequest(t, h, "DeleteMessage", map[string]any{
		"QueueUrl":      queueURL,
		"ReceiptHandle": "invalid-receipt",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerGetQueueURLNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "GetQueueUrl", map[string]any{"QueueName": "nonexistent-queue"})
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
	queueURL := doCreateQueue(t, h, "empty-batch-queue")

	// No batch entries — should return error via writeError.
	rec := doRequest(t, h, "SendMessageBatch", map[string]any{
		"QueueUrl": queueURL,
		"Entries":  []map[string]any{},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp jsonErr
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "com.amazonaws.sqs#EmptyBatchRequest", errResp.Type)
}

func TestHandlerDeleteMessageBatchEmptyEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "empty-del-batch-queue")

	rec := doRequest(t, h, "DeleteMessageBatch", map[string]any{
		"QueueUrl": queueURL,
		"Entries":  []map[string]any{},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp jsonErr
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "com.amazonaws.sqs#EmptyBatchRequest", errResp.Type)
}

func TestHandlerSendMessageBatchTooManyEntries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "toomany-batch-queue")

	entries := make([]map[string]any, 10)
	for i := range 10 {
		entries[i] = map[string]any{
			"Id":          fmt.Sprintf("msg%d", i+1),
			"MessageBody": "body",
		}
	}

	rec := doRequest(t, h, "SendMessageBatch", map[string]any{
		"QueueUrl": queueURL,
		"Entries":  entries,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Successful []struct {
			ID string `json:"Id"`
		} `json:"Successful"`
	}

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Successful, 10)
}

func TestHandlerExtractResourceNoQueueURL(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	body, _ := json.Marshal(map[string]any{"Action": "SendMessage"}) // no QueueUrl
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Empty(t, h.ExtractResource(c))
}

func TestHandlerInvalidBodyParsing(t *testing.T) {
	t.Parallel()

	// An invalid JSON body causes json.Unmarshal to fail in the action handler.
	h := newTestHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{invalid"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS.CreateQueue")
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

func (e *errorBackend) TagQueue(_ *sqs.TagQueueInput) error { return e.err }

func (e *errorBackend) UntagQueue(_ *sqs.UntagQueueInput) error { return e.err }

func (e *errorBackend) ListQueueTags(_ *sqs.ListQueueTagsInput) (*sqs.ListQueueTagsOutput, error) {
	return nil, e.err
}

func (e *errorBackend) ChangeMessageVisibilityBatch(
	_ *sqs.ChangeMessageVisibilityBatchInput,
) (*sqs.ChangeMessageVisibilityBatchOutput, error) {
	return nil, e.err
}

func (e *errorBackend) ListAll() []sqs.QueueInfo { return nil }

func newErrorHandler(t *testing.T, err error) *sqs.Handler {
	t.Helper()

	log := logger.NewLogger(slog.LevelDebug)

	return sqs.NewHandler(&errorBackend{err: err}, log)
}

func TestHandlerListQueuesError(t *testing.T) {
	t.Parallel()

	h := newErrorHandler(t, sqs.ErrQueueNotFound)
	rec := doRequest(t, h, "ListQueues", map[string]any{})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp jsonErr
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "AWS.SimpleQueueService.NonExistentQueue", errResp.Type)
}

func TestHandlerErrorDetailsInvalidAttribute(t *testing.T) {
	t.Parallel()

	h := newErrorHandler(t, sqs.ErrInvalidAttribute)
	rec := doRequest(t, h, "SetQueueAttributes", map[string]any{
		"QueueUrl": "http://localhost/000000000000/q",
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp jsonErr
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "com.amazonaws.sqs#InvalidAttributeValue", errResp.Type)
}

func TestHandlerErrorDetailsTooManyEntriesInBatch(t *testing.T) {
	t.Parallel()

	h := newErrorHandler(t, sqs.ErrTooManyEntriesInBatch)
	rec := doRequest(t, h, "SendMessageBatch", map[string]any{
		"QueueUrl": "http://localhost/000000000000/q",
		"Entries":  []map[string]any{{"Id": "1", "MessageBody": "body"}},
	})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp jsonErr
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "com.amazonaws.sqs#TooManyEntriesInBatchRequest", errResp.Type)
}

func TestHandlerErrorDetailsInternalError(t *testing.T) {
	t.Parallel()

	// Use a non-sentinel error to trigger the default internal error case.
	h := newErrorHandler(t, errInternalTest)
	rec := doRequest(t, h, "PurgeQueue", map[string]any{
		"QueueUrl": "http://localhost/000000000000/q",
	})
	require.Equal(t, http.StatusInternalServerError, rec.Code)

	var errResp jsonErr
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "com.amazonaws.sqs#InternalError", errResp.Type)
}

func TestHandlerExtractOperationInvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	// No X-Amz-Target header → ExtractOperation returns unknownOperation.
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{invalid}"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Equal(t, "Unknown", h.ExtractOperation(c))
}

func TestHandlerExtractResourceInvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{invalid}"))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	c := e.NewContext(req, httptest.NewRecorder())

	assert.Empty(t, h.ExtractResource(c))
}

func TestHandlerQueueNameFromURLEdgeCases(t *testing.T) {
	t.Parallel()

	h := newErrorHandler(t, sqs.ErrQueueNotFound)

	// QueueUrl with an empty URL - queueNameFromURL splits on "/" and returns last part.
	rec := doRequest(t, h, "DeleteQueue", map[string]any{"QueueUrl": ""})
	require.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Handler tests for TagQueue / UntagQueue / ListQueueTags / ChangeMessageVisibilityBatch
// ---------------------------------------------------------------------------

func TestHandlerTagQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "tag-handler-queue")

	rec := doRequest(t, h, "TagQueue", map[string]any{
		"QueueUrl": queueURL,
		"Tags":     map[string]string{"env": "test"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerTagQueue_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRawRequest(t, h, "TagQueue", []byte("{bad json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerUntagQueue(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "untag-handler-queue")

	// First tag it
	rec := doRequest(t, h, "TagQueue", map[string]any{
		"QueueUrl": queueURL,
		"Tags":     map[string]string{"env": "test"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Then untag
	rec = doRequest(t, h, "UntagQueue", map[string]any{
		"QueueUrl": queueURL,
		"TagKeys":  []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerUntagQueue_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRawRequest(t, h, "UntagQueue", []byte("{bad"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerListQueueTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "list-tags-handler-queue")

	rec := doRequest(t, h, "ListQueueTags", map[string]any{"QueueUrl": queueURL})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Tags map[string]string `json:"Tags"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp.Tags)
}

func TestHandlerListQueueTags_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRawRequest(t, h, "ListQueueTags", []byte("{bad"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerChangeMessageVisibilityBatch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "cmvb-handler-queue")

	// Send a message
	sendRec := doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello",
	})
	require.Equal(t, http.StatusOK, sendRec.Code)

	// Receive with visibility=30
	rcvRec := doRequest(t, h, "ReceiveMessage", map[string]any{
		"QueueUrl":            queueURL,
		"MaxNumberOfMessages": 1,
		"VisibilityTimeout":   30,
	})
	require.Equal(t, http.StatusOK, rcvRec.Code)

	var rcvResp struct {
		Messages []struct {
			ReceiptHandle string `json:"ReceiptHandle"`
		} `json:"Messages"`
	}
	require.NoError(t, json.Unmarshal(rcvRec.Body.Bytes(), &rcvResp))
	require.Len(t, rcvResp.Messages, 1)
	handle := rcvResp.Messages[0].ReceiptHandle

	// Batch change visibility to 0
	rec := doRequest(t, h, "ChangeMessageVisibilityBatch", map[string]any{
		"QueueUrl": queueURL,
		"Entries": []map[string]any{
			{"Id": "e1", "ReceiptHandle": handle, "VisibilityTimeout": 0},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerChangeMessageVisibilityBatch_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRawRequest(t, h, "ChangeMessageVisibilityBatch", []byte("{bad"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// doRawRequest sends raw bytes to the handler with the given action header.
func doRawRequest(t *testing.T, h *sqs.Handler, action string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "AmazonSQS."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}
