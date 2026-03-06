package sqs_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

func TestChangeMessageVisibility_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		queueURL string
	}{
		{
			name:     "queue_not_found",
			queueURL: queueURL("nonexistent"),
			wantErr:  sqs.ErrQueueNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			err := b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
				QueueURL:          tt.queueURL,
				ReceiptHandle:     "fake-receipt",
				VisibilityTimeout: 30,
			})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestChangeMessageVisibility_InvalidHandle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
	}{
		{
			name:    "invalid_receipt_handle",
			wantErr: sqs.ErrMessageNotInflight,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			qURL := createTestQueue(t, b, "vis-queue")

			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "hello"})
			require.NoError(t, err)

			recvOut, err := b.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueURL: qURL, MaxNumberOfMessages: 1, VisibilityTimeout: 30,
			})
			require.NoError(t, err)
			require.Len(t, recvOut.Messages, 1)

			err = b.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
				QueueURL:          qURL,
				ReceiptHandle:     "bad-handle",
				VisibilityTimeout: 10,
			})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestUntagQueue_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr  error
		name     string
		queueURL string
	}{
		{
			name:     "queue_not_found",
			queueURL: queueURL("nonexistent-untag"),
			wantErr:  sqs.ErrQueueNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			err := b.UntagQueue(&sqs.UntagQueueInput{
				QueueURL: tt.queueURL,
				TagKeys:  []string{"env"},
			})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}

func TestSQSHandler_ChangeMessageVisibility_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     []byte
		wantCode int
	}{
		{
			name:     "invalid_json",
			body:     []byte("not-json"),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRawRequest(t, h, "ChangeMessageVisibility", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSQSHandler_UntagQueue_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     []byte
		wantCode int
	}{
		{
			name:     "invalid_json",
			body:     []byte("not-json"),
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRawRequest(t, h, "UntagQueue", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSQSHandler_UntagQueue_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "untag_queue_removes_tags",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			qURL := doCreateQueue(t, h, "untag-test-queue")

			doRequest(t, h, "TagQueue", map[string]any{
				"QueueUrl": qURL,
				"Tags":     map[string]string{"env": "test", "team": "platform"},
			})

			rec := doRequest(t, h, "UntagQueue", map[string]any{
				"QueueUrl": qURL,
				"TagKeys":  []string{"team"},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSQSHandler_ChangeMessageVisibility_Success(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "change_visibility_success",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			qURL := doCreateQueue(t, h, "vis-test-queue")

			doRequest(t, h, "SendMessage", map[string]any{
				"QueueUrl":    qURL,
				"MessageBody": "hello",
			})

			recvRec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":            qURL,
				"MaxNumberOfMessages": 1,
				"VisibilityTimeout":   30,
			})
			require.Equal(t, http.StatusOK, recvRec.Code)

			var recvResp struct {
				Messages []struct {
					ReceiptHandle string `json:"ReceiptHandle"`
				} `json:"Messages"`
			}

			require.NoError(t, json.Unmarshal(recvRec.Body.Bytes(), &recvResp))
			require.Len(t, recvResp.Messages, 1)

			rec := doRequest(t, h, "ChangeMessageVisibility", map[string]any{
				"QueueUrl":          qURL,
				"ReceiptHandle":     recvResp.Messages[0].ReceiptHandle,
				"VisibilityTimeout": 0,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestSQSHandler_ExtractResource_InvalidJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "invalid_json_returns_empty",
			body: "not-json",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("X-Amz-Target", "AmazonSQS.GetQueueAttributes")
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}
