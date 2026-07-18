package sqs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
