package sqs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
