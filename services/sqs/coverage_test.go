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

func TestTaggedQueues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantLen  int
		withTags bool
	}{
		{name: "no_queues", wantLen: 0, withTags: false},
		{name: "queue_with_tags_included", wantLen: 1, withTags: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			if tt.withTags {
				qURL := createTestQueue(t, b, "tagged-queue")
				require.NoError(t, b.TagQueue(&sqs.TagQueueInput{
					QueueURL: qURL,
				}))
			}

			result := b.TaggedQueues()
			assert.Len(t, result, tt.wantLen)
		})
	}
}

func TestTagQueueByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		arn     string
	}{
		{
			name:    "not_found_by_arn",
			arn:     "arn:aws:sqs:us-east-1:000000000000:nonexistent",
			wantErr: sqs.ErrQueueNotFound,
		},
		{
			name:    "tags_applied_by_arn",
			arn:     "", // set after queue creation
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			qARN := tt.arn

			if qARN == "" {
				createTestQueue(t, b, "arn-tag-queue")
				attrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
					QueueURL:       queueURL("arn-tag-queue"),
					AttributeNames: []string{"QueueArn"},
				})
				require.NoError(t, err)
				qARN = attrs.Attributes["QueueArn"]
			}

			err := b.TagQueueByARN(qARN, map[string]string{"env": "test"})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestUntagQueueByARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		arn     string
	}{
		{
			name:    "not_found_by_arn",
			arn:     "arn:aws:sqs:us-east-1:000000000000:nonexistent",
			wantErr: sqs.ErrQueueNotFound,
		},
		{
			name:    "tags_removed_by_arn",
			arn:     "", // set after queue creation
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			qARN := tt.arn

			if qARN == "" {
				createTestQueue(t, b, "arn-untag-queue")
				require.NoError(t, b.TagQueue(&sqs.TagQueueInput{QueueURL: queueURL("arn-untag-queue")}))
				attrs, err := b.GetQueueAttributes(&sqs.GetQueueAttributesInput{
					QueueURL:       queueURL("arn-untag-queue"),
					AttributeNames: []string{"QueueArn"},
				})
				require.NoError(t, err)
				qARN = attrs.Attributes["QueueArn"]
			}

			err := b.UntagQueueByARN(qARN, []string{"env"})

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestReceiveAndDeleteMessagesLocal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "local_receive_then_delete"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			qURL := createTestQueue(t, b, "local-ops-queue")

			_, err := b.SendMessage(&sqs.SendMessageInput{QueueURL: qURL, MessageBody: "msg"})
			require.NoError(t, err)

			msgs, err := b.ReceiveMessagesLocal(qURL, 1)
			require.NoError(t, err)
			require.Len(t, msgs, 1)

			handles := make([]string, 0, len(msgs))
			for _, m := range msgs {
				handles = append(handles, m.ReceiptHandle)
			}

			require.NoError(t, b.DeleteMessagesLocal(qURL, handles), tt.name)
		})
	}
}

func TestBackendReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "reset_clears_all_queues"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			createTestQueue(t, b, "q1")
			createTestQueue(t, b, "q2")

			b.Reset()

			out, err := b.ListQueues(&sqs.ListQueuesInput{})
			require.NoError(t, err)
			assert.Empty(t, out.QueueURLs, tt.name)
		})
	}
}

func TestSQSHandler_Reset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "handler_reset_clears_queues"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doCreateQueue(t, h, "before-reset-queue")

			h.Reset()

			rec := doRequest(t, h, "ListQueues", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				QueueURLs []string `json:"QueueUrls"`
			}

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Empty(t, resp.QueueURLs, tt.name)
		})
	}
}

func TestSQSChaosServiceName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		want string
	}{
		{name: "returns_sqs", want: "sqs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, tt.want, h.ChaosServiceName())
		})
	}
}

func TestValidateBatchEntryIDsEmptyID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr error
		name    string
		entries []sqs.SendMessageBatchEntry
	}{
		{
			name:    "empty_id_rejected",
			entries: []sqs.SendMessageBatchEntry{{ID: "", MessageBody: "msg"}},
			wantErr: sqs.ErrInvalidBatchEntry,
		},
		{
			name: "duplicate_ids_rejected",
			entries: []sqs.SendMessageBatchEntry{
				{ID: "dup", MessageBody: "msg1"},
				{ID: "dup", MessageBody: "msg2"},
			},
			wantErr: sqs.ErrBatchEntryIDsNotDistinct,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			qURL := createTestQueue(t, b, "batch-id-queue")

			_, err := b.SendMessageBatch(&sqs.SendMessageBatchInput{
				QueueURL: qURL,
				Entries:  tt.entries,
			})
			require.ErrorIs(t, err, tt.wantErr)
		})
	}
}
