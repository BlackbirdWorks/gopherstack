package sqs_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateQueue_NameValidation asserts InvalidParameterValue for bad queue names.
func TestCreateQueue_NameValidation(t *testing.T) {
	t.Parallel()

	const wantType = "com.amazonaws.sqs#InvalidParameterValue"

	tests := []struct {
		name      string
		queueName string
		wantType  string
		wantCode  int
	}{
		{name: "valid_name", queueName: "my-queue", wantCode: http.StatusOK},
		{name: "empty_name", queueName: "", wantCode: http.StatusBadRequest, wantType: wantType},
		{
			name:      "too_long_81_chars",
			queueName: strings.Repeat("a", 81),
			wantCode:  http.StatusBadRequest,
			wantType:  wantType,
		},
		{name: "invalid_chars_space", queueName: "bad queue", wantCode: http.StatusBadRequest, wantType: wantType},
		{name: "valid_80_chars", queueName: strings.Repeat("a", 80), wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, "CreateQueue", map[string]any{"QueueName": tt.queueName})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp jsonErr
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp.Type)
			}
		})
	}
}

// TestReceiveMessage_MaxNumberOfMessages_Validation asserts InvalidParameterValue for >10.
func TestReceiveMessage_MaxNumberOfMessages_Validation(t *testing.T) {
	t.Parallel()

	const wantType = "com.amazonaws.sqs#InvalidParameterValue"

	tests := []struct {
		name                string
		wantType            string
		maxNumberOfMessages int
		wantCode            int
	}{
		{name: "valid_max_10", maxNumberOfMessages: 10, wantCode: http.StatusOK},
		{name: "valid_max_1", maxNumberOfMessages: 1, wantCode: http.StatusOK},
		{name: "too_many_11", maxNumberOfMessages: 11, wantCode: http.StatusBadRequest, wantType: wantType},
		{name: "zero_uses_default", maxNumberOfMessages: 0, wantCode: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queueURL := doCreateQueue(t, h, "recv-val-queue")

			rec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":            queueURL,
				"MaxNumberOfMessages": tt.maxNumberOfMessages,
			})
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantType != "" {
				var resp jsonErr
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, tt.wantType, resp.Type)
			}
		})
	}
}
