package sqs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sqs"
)

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
