package sqs_test

// accuracy_b4_1677_test.go — batch-4 AWS-accuracy tests for issue #1677.
//
// Covers two accuracy gaps in the ReceiveMessage and SendMessage handlers:
//
//  1. ReceiveMessage VisibilityTimeout range validation: AWS rejects values
//     outside [0, 43200] with InvalidParameterValue. Previously the handler
//     forwarded any value (including negative or > 43200) to the backend,
//     which silently applied it.
//
//  2. SendMessage SequenceNumber in response: AWS only includes SequenceNumber
//     in the SendMessage response for FIFO queues. Previously the standard-queue
//     response always contained "SequenceNumber": "", which is not emitted by
//     the real service.

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// 1. ReceiveMessage — VisibilityTimeout range validation
// ---------------------------------------------------------------------------

// TestB4_ReceiveMessage_VisibilityTimeout_OutOfRange verifies that
// ReceiveMessage returns 400 InvalidParameterValue when VisibilityTimeout is
// outside the AWS-allowed range of [0, 43200], matching the same validation
// that ChangeMessageVisibility enforces.
func TestB4_ReceiveMessage_VisibilityTimeout_OutOfRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		wantErrType       string
		visibilityTimeout int
		wantStatus        int
	}{
		{
			name:              "above_max_43201",
			visibilityTimeout: 43201,
			wantStatus:        http.StatusBadRequest,
			wantErrType:       "com.amazonaws.sqs#InvalidParameterValue",
		},
		{
			name:              "far_above_max",
			visibilityTimeout: 99999,
			wantStatus:        http.StatusBadRequest,
			wantErrType:       "com.amazonaws.sqs#InvalidParameterValue",
		},
		{
			name:              "negative_one",
			visibilityTimeout: -1,
			wantStatus:        http.StatusBadRequest,
			wantErrType:       "com.amazonaws.sqs#InvalidParameterValue",
		},
		{
			name:              "negative_large",
			visibilityTimeout: -100,
			wantStatus:        http.StatusBadRequest,
			wantErrType:       "com.amazonaws.sqs#InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queueURL := doCreateQueue(t, h, "vt-range-queue-"+strings.ReplaceAll(tt.name, "_", "-"))
			doRequest(t, h, "SendMessage", map[string]any{
				"QueueUrl":    queueURL,
				"MessageBody": "test",
			})

			rec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":          queueURL,
				"VisibilityTimeout": tt.visibilityTimeout,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var errResp struct {
				Type string `json:"__type"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
			assert.Equal(t, tt.wantErrType, errResp.Type)
		})
	}
}

// TestB4_ReceiveMessage_VisibilityTimeout_ValidBoundaries verifies that
// ReceiveMessage succeeds with VisibilityTimeout values at the valid boundaries
// (0 and 43200).
func TestB4_ReceiveMessage_VisibilityTimeout_ValidBoundaries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		visibilityTimeout int
	}{
		{name: "zero", visibilityTimeout: 0},
		{name: "max_43200", visibilityTimeout: 43200},
		{name: "mid_range", visibilityTimeout: 30},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			queueURL := doCreateQueue(t, h, "vt-valid-"+strings.ReplaceAll(tt.name, "_", "-"))
			doRequest(t, h, "SendMessage", map[string]any{
				"QueueUrl":    queueURL,
				"MessageBody": "test",
			})

			rec := doRequest(t, h, "ReceiveMessage", map[string]any{
				"QueueUrl":          queueURL,
				"VisibilityTimeout": tt.visibilityTimeout,
			})
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Messages []struct {
					Body string `json:"Body"`
				} `json:"Messages"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Len(t, resp.Messages, 1)
		})
	}
}

// ---------------------------------------------------------------------------
// 2. SendMessage — SequenceNumber absent from standard-queue response
// ---------------------------------------------------------------------------

// TestB4_SendMessage_Standard_SequenceNumberAbsent verifies that the
// SendMessage response for a standard queue does NOT include a SequenceNumber
// field, matching AWS behaviour where SequenceNumber is FIFO-only.
func TestB4_SendMessage_Standard_SequenceNumberAbsent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueURL := doCreateQueue(t, h, "std-seq-absent")

	rec := doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":    queueURL,
		"MessageBody": "hello standard",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Unmarshal into a raw map so we can check key presence, not just value.
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	_, hasSeqNum := raw["SequenceNumber"]
	assert.False(t, hasSeqNum, "standard queue SendMessage response must not contain SequenceNumber")
}

// TestB4_SendMessage_FIFO_SequenceNumberPresent verifies that the SendMessage
// response for a FIFO queue DOES include a non-empty SequenceNumber, confirming
// the omitempty tag does not suppress valid FIFO values.
func TestB4_SendMessage_FIFO_SequenceNumberPresent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateQueue", map[string]any{
		"QueueName": "b4-fifo.fifo",
		"Attributes": map[string]string{
			"FifoQueue":                 "true",
			"ContentBasedDeduplication": "true",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp struct {
		QueueURL string `json:"QueueUrl"`
	}
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))

	rec := doRequest(t, h, "SendMessage", map[string]any{
		"QueueUrl":       createResp.QueueURL,
		"MessageBody":    "hello fifo",
		"MessageGroupId": "grp1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &raw))

	rawSeq, hasSeqNum := raw["SequenceNumber"]
	require.True(t, hasSeqNum, "FIFO queue SendMessage response must contain SequenceNumber")

	var seq string
	require.NoError(t, json.Unmarshal(rawSeq, &seq))
	assert.NotEmpty(t, seq, "SequenceNumber must be non-empty for FIFO queue")
	assert.Len(t, seq, 20, "SequenceNumber must be 20 digits")
}
