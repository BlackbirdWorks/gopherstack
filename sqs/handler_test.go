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

func TestHandlerActions(t *testing.T) {
t.Parallel()

tests := []struct {
name     string
handler  func(t *testing.T) *sqs.Handler
setup    func(t *testing.T, h *sqs.Handler)
action   string
body     map[string]any
wantCode int
want     func(t *testing.T, body []byte)
run      func(t *testing.T)
}{
{
name:     "CreateQueue",
action:   "CreateQueue",
body:     map[string]any{"QueueName": "test-queue"},
wantCode: http.StatusOK,
want: func(t *testing.T, body []byte) {
t.Helper()
var resp struct {
QueueURL string `json:"QueueUrl"`
}
require.NoError(t, json.Unmarshal(body, &resp))
assert.Contains(t, resp.QueueURL, "test-queue")
},
},
{
name: "CreateQueue/duplicate",
setup: func(t *testing.T, h *sqs.Handler) {
t.Helper()
doCreateQueue(t, h, "test-queue")
},
action:   "CreateQueue",
body:     map[string]any{"QueueName": "test-queue"},
wantCode: http.StatusBadRequest,
want: func(t *testing.T, body []byte) {
t.Helper()
var errResp jsonErr
require.NoError(t, json.Unmarshal(body, &errResp))
assert.Equal(t, "com.amazonaws.sqs#QueueNameExists", errResp.Type)
},
},
{
name: "ListQueues",
setup: func(t *testing.T, h *sqs.Handler) {
t.Helper()
doCreateQueue(t, h, "queue-a")
doCreateQueue(t, h, "queue-b")
},
action:   "ListQueues",
body:     map[string]any{},
wantCode: http.StatusOK,
want: func(t *testing.T, body []byte) {
t.Helper()
var resp struct {
QueueURLs []string `json:"QueueUrls"`
}
require.NoError(t, json.Unmarshal(body, &resp))
assert.Len(t, resp.QueueURLs, 2)
},
},
{
name: "ListQueues/with prefix",
setup: func(t *testing.T, h *sqs.Handler) {
t.Helper()
doCreateQueue(t, h, "alpha-queue")
doCreateQueue(t, h, "beta-queue")
},
action:   "ListQueues",
body:     map[string]any{"QueueNamePrefix": "alpha"},
wantCode: http.StatusOK,
want: func(t *testing.T, body []byte) {
t.Helper()
var resp struct {
QueueURLs []string `json:"QueueUrls"`
}
require.NoError(t, json.Unmarshal(body, &resp))
assert.Len(t, resp.QueueURLs, 1)
},
},
{
name: "SendMessage",
run: func(t *testing.T) {
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
require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
assert.NotEmpty(t, resp.MessageID)
assert.NotEmpty(t, resp.MD5OfMessageBody)
},
},
{
name: "ReceiveMessage",
run: func(t *testing.T) {
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
require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
require.Len(t, resp.Messages, 1)
assert.Equal(t, "hello", resp.Messages[0].Body)
},
},
{
name: "ReceiveMessage/with visibility timeout",
run: func(t *testing.T) {
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
require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
assert.Len(t, resp.Messages, 1)
},
},
{
name: "DeleteMessage",
run: func(t *testing.T) {
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
},
},
{
name: "DeleteMessage/not found",
run: func(t *testing.T) {
h := newTestHandler(t)
queueURL := doCreateQueue(t, h, "del-msg-queue")

rec := doRequest(t, h, "DeleteMessage", map[string]any{
"QueueUrl":      queueURL,
"ReceiptHandle": "invalid-receipt",
})
require.Equal(t, http.StatusBadRequest, rec.Code)
},
},
{
name:     "missing action",
action:   "",
body:     map[string]any{"QueueName": "test"},
wantCode: http.StatusBadRequest,
want: func(t *testing.T, body []byte) {
t.Helper()
assert.Contains(t, string(body), "Missing X-Amz-Target")
},
},
{
name:     "unknown action",
action:   "NonExistentAction",
body:     map[string]any{},
wantCode: http.StatusBadRequest,
want: func(t *testing.T, body []byte) {
t.Helper()
var errResp jsonErr
require.NoError(t, json.Unmarshal(body, &errResp))
assert.Equal(t, "com.amazonaws.sqs#InvalidAction", errResp.Type)
},
},
{
name:   "queue not found",
action: "SendMessage",
body: map[string]any{
"QueueUrl":    "http://localhost/000000000000/nonexistent",
"MessageBody": "hello",
},
wantCode: http.StatusBadRequest,
want: func(t *testing.T, body []byte) {
t.Helper()
var errResp jsonErr
require.NoError(t, json.Unmarshal(body, &errResp))
assert.Equal(t, "AWS.SimpleQueueService.NonExistentQueue", errResp.Type)
},
},
{
name: "GetQueueUrl",
setup: func(t *testing.T, h *sqs.Handler) {
t.Helper()
doCreateQueue(t, h, "my-queue")
},
action:   "GetQueueUrl",
body:     map[string]any{"QueueName": "my-queue"},
wantCode: http.StatusOK,
want: func(t *testing.T, body []byte) {
t.Helper()
var resp struct {
QueueURL string `json:"QueueUrl"`
}
require.NoError(t, json.Unmarshal(body, &resp))
assert.Contains(t, resp.QueueURL, "my-queue")
},
},
{
name:     "GetQueueUrl/not found",
action:   "GetQueueUrl",
body:     map[string]any{"QueueName": "nonexistent-queue"},
wantCode: http.StatusBadRequest,
},
{
name: "DeleteQueue",
run: func(t *testing.T) {
h := newTestHandler(t)
queueURL := doCreateQueue(t, h, "del-queue")

rec := doRequest(t, h, "DeleteQueue", map[string]any{"QueueUrl": queueURL})
require.Equal(t, http.StatusOK, rec.Code)
},
},
{
name:     "DeleteQueue/not found",
action:   "DeleteQueue",
body:     map[string]any{"QueueUrl": "http://localhost/000000000000/noqueue"},
wantCode: http.StatusBadRequest,
},
{
name: "PurgeQueue",
run: func(t *testing.T) {
h := newTestHandler(t)
queueURL := doCreateQueue(t, h, "my-queue")

doRequest(t, h, "SendMessage", map[string]any{
"QueueUrl":    queueURL,
"MessageBody": "hello",
})

rec := doRequest(t, h, "PurgeQueue", map[string]any{"QueueUrl": queueURL})
require.Equal(t, http.StatusOK, rec.Code)
},
},
{
name:   "PurgeQueue/not found",
action: "PurgeQueue",
body: map[string]any{
"QueueUrl": "http://localhost/000000000000/noqueue",
},
wantCode: http.StatusBadRequest,
},
{
name: "GetQueueAttributes",
run: func(t *testing.T) {
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
require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
assert.NotEmpty(t, resp.Attributes)
},
},
{
name:     "GetQueueAttributes/not found",
action:   "GetQueueAttributes",
body:     map[string]any{"QueueUrl": "http://localhost/000000000000/noqueue"},
wantCode: http.StatusBadRequest,
},
{
name: "SetQueueAttributes",
run: func(t *testing.T) {
h := newTestHandler(t)
queueURL := doCreateQueue(t, h, "set-attr-queue")

rec := doRequest(t, h, "SetQueueAttributes", map[string]any{
"QueueUrl":   queueURL,
"Attributes": map[string]string{"VisibilityTimeout": "60"},
})
require.Equal(t, http.StatusOK, rec.Code)
},
},
{
name:   "SetQueueAttributes/not found",
action: "SetQueueAttributes",
body: map[string]any{
"QueueUrl":   "http://localhost/000000000000/noqueue",
"Attributes": map[string]string{"VisibilityTimeout": "60"},
},
wantCode: http.StatusBadRequest,
},
{
name: "ChangeMessageVisibility",
run: func(t *testing.T) {
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
},
},
{
name: "ChangeMessageVisibility/not found",
run: func(t *testing.T) {
h := newTestHandler(t)
queueURL := doCreateQueue(t, h, "vis-queue")

rec := doRequest(t, h, "ChangeMessageVisibility", map[string]any{
"QueueUrl":          queueURL,
"ReceiptHandle":     "invalid-receipt",
"VisibilityTimeout": 10,
})
require.Equal(t, http.StatusBadRequest, rec.Code)
},
},
{
name: "SendMessageBatch",
run: func(t *testing.T) {
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
require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
assert.Len(t, resp.Successful, 2)
},
},
{
name:   "SendMessageBatch/not found",
action: "SendMessageBatch",
body: map[string]any{
"QueueUrl": "http://localhost/000000000000/noqueue",
"Entries":  []map[string]any{{"Id": "msg1", "MessageBody": "hello"}},
},
wantCode: http.StatusOK,
want: func(t *testing.T, body []byte) {
t.Helper()
var resp struct {
Failed []struct {
ID string `json:"Id"`
} `json:"Failed"`
}
require.NoError(t, json.Unmarshal(body, &resp))
assert.Len(t, resp.Failed, 1)
},
},
{
name: "SendMessageBatch/empty entries",
run: func(t *testing.T) {
h := newTestHandler(t)
queueURL := doCreateQueue(t, h, "empty-batch-queue")

rec := doRequest(t, h, "SendMessageBatch", map[string]any{
"QueueUrl": queueURL,
"Entries":  []map[string]any{},
})
require.Equal(t, http.StatusBadRequest, rec.Code)

var errResp jsonErr
require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
assert.Equal(t, "com.amazonaws.sqs#EmptyBatchRequest", errResp.Type)
},
},
{
name: "SendMessageBatch/too many entries",
run: func(t *testing.T) {
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
},
},
{
name: "DeleteMessageBatch",
run: func(t *testing.T) {
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
require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
assert.Len(t, resp.Successful, 1)
},
},
{
name:   "DeleteMessageBatch/not found",
action: "DeleteMessageBatch",
body: map[string]any{
"QueueUrl": "http://localhost/000000000000/noqueue",
"Entries":  []map[string]any{{"Id": "entry1", "ReceiptHandle": "some-receipt"}},
},
wantCode: http.StatusOK,
want: func(t *testing.T, body []byte) {
t.Helper()
var resp struct {
Failed []struct {
ID string `json:"Id"`
} `json:"Failed"`
}
require.NoError(t, json.Unmarshal(body, &resp))
assert.Len(t, resp.Failed, 1)
},
},
{
name: "DeleteMessageBatch/failed entry",
run: func(t *testing.T) {
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
require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
assert.Len(t, resp.Failed, 1)
},
},
{
name: "DeleteMessageBatch/empty entries",
run: func(t *testing.T) {
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
},
},
{
name: "TagQueue",
run: func(t *testing.T) {
h := newTestHandler(t)
queueURL := doCreateQueue(t, h, "tag-handler-queue")

rec := doRequest(t, h, "TagQueue", map[string]any{
"QueueUrl": queueURL,
"Tags":     map[string]string{"env": "test"},
})
assert.Equal(t, http.StatusOK, rec.Code)
},
},
{
name: "TagQueue/invalid body",
run: func(t *testing.T) {
h := newTestHandler(t)
rec := doRawRequest(t, h, "TagQueue", []byte("{bad json"))
assert.Equal(t, http.StatusBadRequest, rec.Code)
},
},
{
name: "UntagQueue",
run: func(t *testing.T) {
h := newTestHandler(t)
queueURL := doCreateQueue(t, h, "untag-handler-queue")

rec := doRequest(t, h, "TagQueue", map[string]any{
"QueueUrl": queueURL,
"Tags":     map[string]string{"env": "test"},
})
require.Equal(t, http.StatusOK, rec.Code)

rec = doRequest(t, h, "UntagQueue", map[string]any{
"QueueUrl": queueURL,
"TagKeys":  []string{"env"},
})
assert.Equal(t, http.StatusOK, rec.Code)
},
},
{
name: "UntagQueue/invalid body",
run: func(t *testing.T) {
h := newTestHandler(t)
rec := doRawRequest(t, h, "UntagQueue", []byte("{bad"))
assert.Equal(t, http.StatusBadRequest, rec.Code)
},
},
{
name: "ListQueueTags",
run: func(t *testing.T) {
h := newTestHandler(t)
queueURL := doCreateQueue(t, h, "list-tags-handler-queue")

rec := doRequest(t, h, "ListQueueTags", map[string]any{"QueueUrl": queueURL})
require.Equal(t, http.StatusOK, rec.Code)

var resp struct {
Tags map[string]string `json:"Tags"`
}
require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
assert.NotNil(t, resp.Tags)
},
},
{
name: "ListQueueTags/invalid body",
run: func(t *testing.T) {
h := newTestHandler(t)
rec := doRawRequest(t, h, "ListQueueTags", []byte("{bad"))
assert.Equal(t, http.StatusBadRequest, rec.Code)
},
},
{
name: "ChangeMessageVisibilityBatch",
run: func(t *testing.T) {
h := newTestHandler(t)
queueURL := doCreateQueue(t, h, "cmvb-handler-queue")

sendRec := doRequest(t, h, "SendMessage", map[string]any{
"QueueUrl":    queueURL,
"MessageBody": "hello",
})
require.Equal(t, http.StatusOK, sendRec.Code)

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

rec := doRequest(t, h, "ChangeMessageVisibilityBatch", map[string]any{
"QueueUrl": queueURL,
"Entries": []map[string]any{
{"Id": "e1", "ReceiptHandle": handle, "VisibilityTimeout": 0},
},
})
assert.Equal(t, http.StatusOK, rec.Code)
},
},
{
name: "ChangeMessageVisibilityBatch/invalid body",
run: func(t *testing.T) {
h := newTestHandler(t)
rec := doRawRequest(t, h, "ChangeMessageVisibilityBatch", []byte("{bad"))
assert.Equal(t, http.StatusBadRequest, rec.Code)
},
},
{
name: "invalid body parsing",
run: func(t *testing.T) {
h := newTestHandler(t)
rec := doRawRequest(t, h, "CreateQueue", []byte("{invalid"))
assert.Equal(t, http.StatusBadRequest, rec.Code)
},
},
{
name: "queue name from URL edge cases",
handler: func(t *testing.T) *sqs.Handler {
t.Helper()
return newErrorHandler(t, sqs.ErrQueueNotFound)
},
action:   "DeleteQueue",
body:     map[string]any{"QueueUrl": ""},
wantCode: http.StatusBadRequest,
},
// Error backend tests
{
name: "ListQueues/error backend",
handler: func(t *testing.T) *sqs.Handler {
t.Helper()
return newErrorHandler(t, sqs.ErrQueueNotFound)
},
action:   "ListQueues",
body:     map[string]any{},
wantCode: http.StatusBadRequest,
want: func(t *testing.T, body []byte) {
t.Helper()
var errResp jsonErr
require.NoError(t, json.Unmarshal(body, &errResp))
assert.Equal(t, "AWS.SimpleQueueService.NonExistentQueue", errResp.Type)
},
},
{
name: "error details/invalid attribute",
handler: func(t *testing.T) *sqs.Handler {
t.Helper()
return newErrorHandler(t, sqs.ErrInvalidAttribute)
},
action:   "SetQueueAttributes",
body:     map[string]any{"QueueUrl": "http://localhost/000000000000/q"},
wantCode: http.StatusBadRequest,
want: func(t *testing.T, body []byte) {
t.Helper()
var errResp jsonErr
require.NoError(t, json.Unmarshal(body, &errResp))
assert.Equal(t, "com.amazonaws.sqs#InvalidAttributeValue", errResp.Type)
},
},
{
name: "error details/too many entries in batch",
handler: func(t *testing.T) *sqs.Handler {
t.Helper()
return newErrorHandler(t, sqs.ErrTooManyEntriesInBatch)
},
action: "SendMessageBatch",
body: map[string]any{
"QueueUrl": "http://localhost/000000000000/q",
"Entries":  []map[string]any{{"Id": "1", "MessageBody": "body"}},
},
wantCode: http.StatusBadRequest,
want: func(t *testing.T, body []byte) {
t.Helper()
var errResp jsonErr
require.NoError(t, json.Unmarshal(body, &errResp))
assert.Equal(t, "com.amazonaws.sqs#TooManyEntriesInBatchRequest", errResp.Type)
},
},
{
name: "error details/internal error",
handler: func(t *testing.T) *sqs.Handler {
t.Helper()
return newErrorHandler(t, errInternalTest)
},
action:   "PurgeQueue",
body:     map[string]any{"QueueUrl": "http://localhost/000000000000/q"},
wantCode: http.StatusInternalServerError,
want: func(t *testing.T, body []byte) {
t.Helper()
var errResp jsonErr
require.NoError(t, json.Unmarshal(body, &errResp))
assert.Equal(t, "com.amazonaws.sqs#InternalError", errResp.Type)
},
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()

if tt.run != nil {
tt.run(t)
return
}

h := newTestHandler(t)
if tt.handler != nil {
h = tt.handler(t)
}

if tt.setup != nil {
tt.setup(t, h)
}

rec := doRequest(t, h, tt.action, tt.body)
require.Equal(t, tt.wantCode, rec.Code)

if tt.want != nil {
tt.want(t, rec.Body.Bytes())
}
})
}
}

func TestHandlerRouteMatcher(t *testing.T) {
t.Parallel()

h := newTestHandler(t)
matcher := h.RouteMatcher()
e := echo.New()

tests := []struct {
name   string
path   string
target string // X-Amz-Target header; "" means omit
want   bool
}{
{"match root path with SQS target", "/", "AmazonSQS.CreateQueue", true},
{"match queue path with SQS target", "/000000000000/my-queue", "AmazonSQS.SendMessage", true},
{"no match missing X-Amz-Target", "/", "", false},
{"no match wrong path with SQS target", "/dashboard/sqs/create", "AmazonSQS.CreateQueue", false},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()

req := httptest.NewRequest(http.MethodPost, tt.path, nil)
if tt.target != "" {
req.Header.Set("X-Amz-Target", tt.target)
}

c := e.NewContext(req, httptest.NewRecorder())
assert.Equal(t, tt.want, matcher(c))
})
}
}

func TestHandlerIntrospection(t *testing.T) {
t.Parallel()

tests := []struct {
name string
run  func(t *testing.T)
}{
{
name: "Name",
run: func(t *testing.T) {
h := newTestHandler(t)
assert.Equal(t, "SQS", h.Name())
},
},
{
name: "SupportedOperations",
run: func(t *testing.T) {
h := newTestHandler(t)
assert.NotEmpty(t, h.GetSupportedOperations())
},
},
{
name: "MatchPriority",
run: func(t *testing.T) {
h := newTestHandler(t)
assert.Equal(t, 75, h.MatchPriority())
},
},
{
name: "ExtractOperation/valid",
run: func(t *testing.T) {
h := newTestHandler(t)
e := echo.New()

body, _ := json.Marshal(map[string]any{"QueueUrl": "http://x/000000000000/q"})
req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
req.Header.Set("Content-Type", "application/x-amz-json-1.0")
req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
c := e.NewContext(req, httptest.NewRecorder())

assert.Equal(t, "SendMessage", h.ExtractOperation(c))
},
},
{
name: "ExtractResource/valid",
run: func(t *testing.T) {
h := newTestHandler(t)
e := echo.New()

body, _ := json.Marshal(map[string]any{"QueueUrl": "http://x/000000000000/q"})
req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
req.Header.Set("Content-Type", "application/x-amz-json-1.0")
req.Header.Set("X-Amz-Target", "AmazonSQS.SendMessage")
c := e.NewContext(req, httptest.NewRecorder())

assert.Equal(t, "q", h.ExtractResource(c))
},
},
{
name: "ExtractOperation/invalid body",
run: func(t *testing.T) {
h := newTestHandler(t)
e := echo.New()

req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{invalid}"))
req.Header.Set("Content-Type", "application/x-amz-json-1.0")
c := e.NewContext(req, httptest.NewRecorder())

assert.Equal(t, "Unknown", h.ExtractOperation(c))
},
},
{
name: "ExtractResource/no QueueUrl",
run: func(t *testing.T) {
h := newTestHandler(t)
e := echo.New()

body, _ := json.Marshal(map[string]any{"Action": "SendMessage"})
req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
req.Header.Set("Content-Type", "application/x-amz-json-1.0")
c := e.NewContext(req, httptest.NewRecorder())

assert.Empty(t, h.ExtractResource(c))
},
},
{
name: "ExtractResource/invalid body",
run: func(t *testing.T) {
h := newTestHandler(t)
e := echo.New()

req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{invalid}"))
req.Header.Set("Content-Type", "application/x-amz-json-1.0")
c := e.NewContext(req, httptest.NewRecorder())

assert.Empty(t, h.ExtractResource(c))
},
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()
tt.run(t)
})
}
}

func TestProviderNameAndInit(t *testing.T) {
t.Parallel()

tests := []struct {
name string
run  func(t *testing.T)
}{
{
name: "Name",
run: func(t *testing.T) {
p := &sqs.Provider{}
assert.Equal(t, "SQS", p.Name())
},
},
{
name: "Init",
run: func(t *testing.T) {
p := &sqs.Provider{}
log := logger.NewLogger(slog.LevelDebug)
appCtx := &service.AppContext{Logger: log}

svc, err := p.Init(appCtx)
require.NoError(t, err)
assert.NotNil(t, svc)
},
},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()
tt.run(t)
})
}
}
