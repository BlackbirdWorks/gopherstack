package sqs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/sqs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			name: "duplicate_same_attrs",
			setup: func(t *testing.T, h *sqs.Handler) {
				t.Helper()
				doCreateQueue(t, h, "test-queue")
			},
			body:            map[string]any{"QueueName": "test-queue"},
			wantCode:        http.StatusOK,
			wantBodyContain: "test-queue",
		},
		{
			name: "duplicate_diff_attrs",
			setup: func(t *testing.T, h *sqs.Handler) {
				t.Helper()
				doCreateQueue(t, h, "test-queue")
			},
			body: map[string]any{
				"QueueName":  "test-queue",
				"Attributes": map[string]string{"VisibilityTimeout": "60"},
			},
			wantCode:        http.StatusBadRequest,
			wantBodyContain: "QueueNameExists",
		},
		{
			name:     "invalid_name",
			body:     map[string]any{"QueueName": "invalid name!"},
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
