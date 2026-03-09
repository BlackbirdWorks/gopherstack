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

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sqs"
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

	backend := sqs.NewInMemoryBackend()

	return sqs.NewHandler(backend)
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

func (e *errorBackend) ListDeadLetterSourceQueues(
	_ *sqs.ListDeadLetterSourceQueuesInput,
) (*sqs.ListDeadLetterSourceQueuesOutput, error) {
	return nil, e.err
}

func (e *errorBackend) ListAll() []sqs.QueueInfo { return nil }

func newErrorHandler(t *testing.T, err error) *sqs.Handler {
	t.Helper()

	return sqs.NewHandler(&errorBackend{err: err})
}

// --- Handler action tests ---

func TestHandlerActions_Routing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body            map[string]any
		name            string
		action          string
		wantBodyContain string
		wantCode        int
	}{
		{
			name:            "missing action",
			action:          "",
			body:            map[string]any{"QueueName": "test"},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "Missing X-Amz-Target",
		},
		{
			name:            "unknown action",
			action:          "NonExistentAction",
			body:            map[string]any{},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "InvalidAction",
		},
		{
			name:   "queue not found",
			action: "SendMessage",
			body: map[string]any{
				"QueueUrl":    "http://localhost/000000000000/nonexistent",
				"MessageBody": "hello",
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "QueueDoesNotExist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBodyContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContain)
			}
		})
	}
}

func TestHandlerActions_CreateQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *sqs.Handler)
		body            map[string]any
		name            string
		wantBodyContain string
		wantCode        int
	}{
		{
			name:            "success",
			body:            map[string]any{"QueueName": "test-queue"},
			wantCode:        http.StatusOK,
			wantBodyContain: "test-queue",
		},
		{
			name: "duplicate",
			setup: func(t *testing.T, h *sqs.Handler) {
				t.Helper()
				doCreateQueue(t, h, "test-queue")
			},
			body:            map[string]any{"QueueName": "test-queue"},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "QueueNameExists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, "CreateQueue", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBodyContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContain)
			}
		})
	}
}

func TestHandlerActions_ListQueues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *sqs.Handler)
		body         map[string]any
		name         string
		wantCode     int
		wantURLCount int
	}{
		{
			name: "all queues",
			setup: func(t *testing.T, h *sqs.Handler) {
				t.Helper()
				doCreateQueue(t, h, "queue-a")
				doCreateQueue(t, h, "queue-b")
			},
			body:         map[string]any{},
			wantCode:     http.StatusOK,
			wantURLCount: 2,
		},
		{
			name: "with prefix",
			setup: func(t *testing.T, h *sqs.Handler) {
				t.Helper()
				doCreateQueue(t, h, "alpha-queue")
				doCreateQueue(t, h, "beta-queue")
			},
			body:         map[string]any{"QueueNamePrefix": "alpha"},
			wantCode:     http.StatusOK,
			wantURLCount: 1,
		},
		{
			name: "pagination with max results",
			setup: func(t *testing.T, h *sqs.Handler) {
				t.Helper()
				doCreateQueue(t, h, "page-queue-a")
				doCreateQueue(t, h, "page-queue-b")
				doCreateQueue(t, h, "page-queue-c")
			},
			body:         map[string]any{"MaxResults": 2},
			wantCode:     http.StatusOK,
			wantURLCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, "ListQueues", tt.body)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp struct {
				NextToken string   `json:"NextToken"`
				QueueURLs []string `json:"QueueUrls"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.QueueURLs, tt.wantURLCount)
		})
	}
}

func TestHandlerActions_ListQueues_PaginationRoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doCreateQueue(t, h, "rtp-queue-a")
	doCreateQueue(t, h, "rtp-queue-b")
	doCreateQueue(t, h, "rtp-queue-c")

	var allURLs []string
	var nextToken string

	for {
		body := map[string]any{"MaxResults": 2}
		if nextToken != "" {
			body["NextToken"] = nextToken
		}

		rec := doRequest(t, h, "ListQueues", body)
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			NextToken string   `json:"NextToken"`
			QueueURLs []string `json:"QueueUrls"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		allURLs = append(allURLs, resp.QueueURLs...)
		nextToken = resp.NextToken

		if nextToken == "" {
			break
		}
	}

	assert.Len(t, allURLs, 3)
}

func TestHandlerActions_GetQueueUrl(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup           func(t *testing.T, h *sqs.Handler)
		body            map[string]any
		name            string
		wantBodyContain string
		wantCode        int
	}{
		{
			name: "found",
			setup: func(t *testing.T, h *sqs.Handler) {
				t.Helper()
				doCreateQueue(t, h, "my-queue")
			},
			body:            map[string]any{"QueueName": "my-queue"},
			wantCode:        http.StatusOK,
			wantBodyContain: "my-queue",
		},
		{
			name:     "not found",
			body:     map[string]any{"QueueName": "nonexistent-queue"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rec := doRequest(t, h, "GetQueueUrl", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBodyContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContain)
			}
		})
	}
}

func TestHandlerActions_NotFoundErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "DeleteQueue",
			action: "DeleteQueue",
			body:   map[string]any{"QueueUrl": "http://localhost/000000000000/noqueue"},
		},
		{
			name:   "PurgeQueue",
			action: "PurgeQueue",
			body:   map[string]any{"QueueUrl": "http://localhost/000000000000/noqueue"},
		},
		{
			name:   "GetQueueAttributes",
			action: "GetQueueAttributes",
			body:   map[string]any{"QueueUrl": "http://localhost/000000000000/noqueue"},
		},
		{
			name:   "SetQueueAttributes",
			action: "SetQueueAttributes",
			body: map[string]any{
				"QueueUrl":   "http://localhost/000000000000/noqueue",
				"Attributes": map[string]string{"VisibilityTimeout": "60"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandlerActions_QueueManagement(t *testing.T) {
	t.Parallel()

	t.Run("DeleteQueue", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "del-queue")

		rec := doRequest(t, h, "DeleteQueue", map[string]any{"QueueUrl": queueURL})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("PurgeQueue", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "my-queue")

		doRequest(t, h, "SendMessage", map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "hello",
		})

		rec := doRequest(t, h, "PurgeQueue", map[string]any{"QueueUrl": queueURL})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("GetQueueAttributes", func(t *testing.T) {
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
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.NotEmpty(t, resp.Attributes)
	})

	t.Run("SetQueueAttributes", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "set-attr-queue")

		rec := doRequest(t, h, "SetQueueAttributes", map[string]any{
			"QueueUrl":   queueURL,
			"Attributes": map[string]string{"VisibilityTimeout": "60"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestHandlerActions_SendMessage(t *testing.T) {
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
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp.MessageID)
	assert.NotEmpty(t, resp.MD5OfMessageBody)
}

func TestHandlerActions_ReceiveMessage(t *testing.T) {
	t.Parallel()

	t.Run("standard", func(t *testing.T) {
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
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		require.Len(t, resp.Messages, 1)
		assert.Equal(t, "hello", resp.Messages[0].Body)
	})

	t.Run("with visibility timeout", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "vt-queue")

		doRequest(t, h, "SendMessage", map[string]any{
			"QueueUrl":    queueURL,
			"MessageBody": "hello",
		})

		rec := doRequest(t, h, "ReceiveMessage", map[string]any{
			"QueueUrl":          queueURL,
			"VisibilityTimeout": 30,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			Messages []struct {
				Body string `json:"Body"`
			} `json:"Messages"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Messages, 1)
	})
}

func TestHandlerActions_DeleteMessage(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
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
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "del-msg-queue")

		rec := doRequest(t, h, "DeleteMessage", map[string]any{
			"QueueUrl":      queueURL,
			"ReceiptHandle": "invalid-receipt",
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandlerActions_ChangeMessageVisibility(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
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
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "vis-queue")

		rec := doRequest(t, h, "ChangeMessageVisibility", map[string]any{
			"QueueUrl":          queueURL,
			"ReceiptHandle":     "invalid-receipt",
			"VisibilityTimeout": 10,
		})
		require.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandlerActions_SendMessageBatch(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
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
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Successful, 2)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rec := doRequest(t, h, "SendMessageBatch", map[string]any{
			"QueueUrl": "http://localhost/000000000000/noqueue",
			"Entries":  []map[string]any{{"Id": "msg1", "MessageBody": "hello"}},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			Failed []struct {
				ID string `json:"Id"`
			} `json:"Failed"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Failed, 1)
	})

	t.Run("empty entries", func(t *testing.T) {
		t.Parallel()

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
	})

	t.Run("too many entries", func(t *testing.T) {
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
	})
}

func TestHandlerActions_DeleteMessageBatch(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
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
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Successful, 1)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		rec := doRequest(t, h, "DeleteMessageBatch", map[string]any{
			"QueueUrl": "http://localhost/000000000000/noqueue",
			"Entries":  []map[string]any{{"Id": "entry1", "ReceiptHandle": "some-receipt"}},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp struct {
			Failed []struct {
				ID string `json:"Id"`
			} `json:"Failed"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Failed, 1)
	})

	t.Run("failed entry", func(t *testing.T) {
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
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp.Failed, 1)
	})

	t.Run("empty entries", func(t *testing.T) {
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
	})
}

func TestHandlerActions_TagOps(t *testing.T) {
	t.Parallel()

	t.Run("TagQueue", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		queueURL := doCreateQueue(t, h, "tag-handler-queue")

		rec := doRequest(t, h, "TagQueue", map[string]any{
			"QueueUrl": queueURL,
			"Tags":     map[string]string{"env": "test"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("TagQueue/invalid body", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRawRequest(t, h, "TagQueue", []byte("{bad json"))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("UntagQueue", func(t *testing.T) {
		t.Parallel()

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
	})

	t.Run("UntagQueue/invalid body", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRawRequest(t, h, "UntagQueue", []byte("{bad"))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("ListQueueTags", func(t *testing.T) {
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
	})

	t.Run("ListQueueTags/invalid body", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRawRequest(t, h, "ListQueueTags", []byte("{bad"))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandlerActions_ChangeMessageVisibilityBatch(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()

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
	})

	t.Run("invalid body", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doRawRequest(t, h, "ChangeMessageVisibilityBatch", []byte("{bad"))
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestHandlerActions_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRawRequest(t, h, "CreateQueue", []byte("{invalid"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandlerActions_ErrorBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		backendErr      error
		body            map[string]any
		name            string
		action          string
		wantBodyContain string
		wantCode        int
	}{
		{
			name:       "queue name from URL edge cases",
			backendErr: sqs.ErrQueueNotFound,
			action:     "DeleteQueue",
			body:       map[string]any{"QueueUrl": ""},
			wantCode:   http.StatusBadRequest,
		},
		{
			name:            "ListQueues",
			backendErr:      sqs.ErrQueueNotFound,
			action:          "ListQueues",
			body:            map[string]any{},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "QueueDoesNotExist",
		},
		{
			name:            "invalid attribute",
			backendErr:      sqs.ErrInvalidAttribute,
			action:          "SetQueueAttributes",
			body:            map[string]any{"QueueUrl": "http://localhost/000000000000/q"},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "InvalidAttributeValue",
		},
		{
			name:       "too many entries in batch",
			backendErr: sqs.ErrTooManyEntriesInBatch,
			action:     "SendMessageBatch",
			body: map[string]any{
				"QueueUrl": "http://localhost/000000000000/q",
				"Entries":  []map[string]any{{"Id": "1", "MessageBody": "body"}},
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "TooManyEntriesInBatchRequest",
		},
		{
			name:            "internal error",
			backendErr:      errInternalTest,
			action:          "PurgeQueue",
			body:            map[string]any{"QueueUrl": "http://localhost/000000000000/q"},
			wantCode:        http.StatusInternalServerError,
			wantBodyContain: "InternalError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newErrorHandler(t, tt.backendErr)
			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantBodyContain != "" {
				assert.Contains(t, rec.Body.String(), tt.wantBodyContain)
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

	h := newTestHandler(t)

	assert.Equal(t, "SQS", h.Name())
	assert.Equal(t, 75, h.MatchPriority())
	assert.NotEmpty(t, h.GetSupportedOperations())

	t.Run("ExtractOperation", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			target string
			body   string
			want   string
		}{
			{
				name:   "valid",
				target: "AmazonSQS.SendMessage",
				body:   `{"QueueUrl":"http://x/000000000000/q"}`,
				want:   "SendMessage",
			},
			{
				name: "invalid body",
				body: "{invalid}",
				want: "Unknown",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
				req.Header.Set("Content-Type", "application/x-amz-json-1.0")

				if tt.target != "" {
					req.Header.Set("X-Amz-Target", tt.target)
				}

				c := e.NewContext(req, httptest.NewRecorder())
				assert.Equal(t, tt.want, h.ExtractOperation(c))
			})
		}
	})

	t.Run("ExtractResource", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name   string
			target string
			body   string
			want   string
		}{
			{
				name:   "valid",
				target: "AmazonSQS.SendMessage",
				body:   `{"QueueUrl":"http://x/000000000000/q"}`,
				want:   "q",
			},
			{
				name: "no QueueUrl",
				body: `{"Action":"SendMessage"}`,
				want: "",
			},
			{
				name: "invalid body",
				body: "{invalid}",
				want: "",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				e := echo.New()
				req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
				req.Header.Set("Content-Type", "application/x-amz-json-1.0")

				if tt.target != "" {
					req.Header.Set("X-Amz-Target", tt.target)
				}

				c := e.NewContext(req, httptest.NewRecorder())
				assert.Equal(t, tt.want, h.ExtractResource(c))
			})
		}
	})
}

func TestHandlerActions_ListDeadLetterSourceQueues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(t *testing.T, h *sqs.Handler) string
		name         string
		wantCode     int
		wantURLCount int
	}{
		{
			name: "two_source_queues",
			setup: func(t *testing.T, h *sqs.Handler) string {
				t.Helper()

				dlqURL := doCreateQueue(t, h, "handler-dlq")

				rec := doRequest(t, h, "GetQueueAttributes", map[string]any{
					"QueueUrl":       dlqURL,
					"AttributeNames": []string{"QueueArn"},
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var attrResp struct {
					Attributes map[string]string `json:"Attributes"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &attrResp))
				dlqARN := attrResp.Attributes["QueueArn"]

				policy := `{"deadLetterTargetArn":"` + dlqARN + `","maxReceiveCount":3}`

				srcAURL := doCreateQueue(t, h, "handler-src-a")
				srcBURL := doCreateQueue(t, h, "handler-src-b")

				rec2 := doRequest(t, h, "SetQueueAttributes", map[string]any{
					"QueueUrl":   srcAURL,
					"Attributes": map[string]string{"RedrivePolicy": policy},
				})
				require.Equal(t, http.StatusOK, rec2.Code)

				rec3 := doRequest(t, h, "SetQueueAttributes", map[string]any{
					"QueueUrl":   srcBURL,
					"Attributes": map[string]string{"RedrivePolicy": policy},
				})
				require.Equal(t, http.StatusOK, rec3.Code)

				return dlqURL
			},
			wantCode:     http.StatusOK,
			wantURLCount: 2,
		},
		{
			name: "no_source_queues",
			setup: func(t *testing.T, h *sqs.Handler) string {
				t.Helper()

				return doCreateQueue(t, h, "empty-dlq")
			},
			wantCode:     http.StatusOK,
			wantURLCount: 0,
		},
		{
			name: "queue_not_found",
			setup: func(_ *testing.T, _ *sqs.Handler) string {
				return "http:///000000000000/missing-dlq"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			dlqURL := tt.setup(t, h)

			rec := doRequest(t, h, "ListDeadLetterSourceQueues", map[string]any{
				"QueueUrl": dlqURL,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp struct {
					NextToken string   `json:"NextToken"`
					QueueURLs []string `json:"queueUrls"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Len(t, resp.QueueURLs, tt.wantURLCount)
			}
		})
	}
}

func TestHandlerActions_ListDeadLetterSourceQueues_ErrorBackend(t *testing.T) {
	t.Parallel()

	h := newErrorHandler(t, sqs.ErrQueueNotFound)
	rec := doRequest(t, h, "ListDeadLetterSourceQueues", map[string]any{
		"QueueUrl": "http:///000000000000/any-dlq",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "QueueDoesNotExist")
}

func TestHandlerActions_ListDeadLetterSourceQueues_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRawRequest(t, h, "ListDeadLetterSourceQueues", []byte("not-json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestProviderNameAndInit(t *testing.T) {
	t.Parallel()

	p := &sqs.Provider{}
	assert.Equal(t, "SQS", p.Name())

	appCtx := &service.AppContext{Logger: slog.Default()}

	svc, err := p.Init(appCtx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}
