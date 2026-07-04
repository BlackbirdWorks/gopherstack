package firehose_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Error response __type field ---

// Real AWS Firehose returns __type in every error response so SDK clients can
// deserialize errors into typed structs.  The handler must include __type for
// 400-class errors, not only 404s.

func TestAccuracy_ErrorResponse_ResourceInUseException_HasType(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "dup-stream")

	rec := doFirehoseRequest(t, h, "CreateDeliveryStream",
		map[string]any{"DeliveryStreamName": "dup-stream"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceInUseException", body["__type"],
		"duplicate-stream error must carry __type=ResourceInUseException")
}

func TestAccuracy_ErrorResponse_InvalidArgumentException_HasType(t *testing.T) {
	t.Parallel()

	longKey := make([]byte, 129)
	for i := range longKey {
		longKey[i] = 'x'
	}

	tests := []struct {
		body   map[string]any
		name   string
		action string
	}{
		{
			name:   "empty_record_data",
			action: "PutRecord",
			body: map[string]any{
				"DeliveryStreamName": "invalid-stream",
				"Record":             map[string]any{"Data": ""},
			},
		},
		{
			name:   "tag_key_too_long",
			action: "CreateDeliveryStream",
			body: map[string]any{
				"DeliveryStreamName": "tag-err-stream",
				"Tags": []map[string]string{
					{"Key": string(longKey), "Value": "v"},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)
			// Pre-create the stream for PutRecord tests.
			if tt.action == "PutRecord" {
				createStream(t, h, "invalid-stream")
			}

			rec := doFirehoseRequest(t, h, tt.action, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "InvalidArgumentException", body["__type"],
				"validation error must carry __type=InvalidArgumentException; got: %v", body)
		})
	}
}

func TestAccuracy_ErrorResponse_ResourceNotFoundException_HasType(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "DescribeDeliveryStream",
		map[string]any{"DeliveryStreamName": "no-such-stream"})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "ResourceNotFoundException", body["__type"],
		"not-found error must carry __type=ResourceNotFoundException")
}

func TestAccuracy_ErrorResponse_UnknownOperation_HasType(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "NoSuchAction", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "UnknownOperationException", body["__type"],
		"unknown operation must carry __type=UnknownOperationException")
}

// --- PutRecordBatch per-record RecordId ---

// Real AWS PutRecordBatch returns a RequestResponses array with one entry per
// input record.  Each successful entry carries a RecordId; each failed entry
// carries ErrorCode and ErrorMessage.  Returning empty structs loses the
// per-record ID that callers use for tracking and deduplication.

func TestAccuracy_PutRecordBatch_PerRecordRecordId(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		recordCount int
	}{
		{name: "single_record", recordCount: 1},
		{name: "three_records", recordCount: 3},
		{name: "ten_records", recordCount: 10},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)
			createStream(t, h, "batch-id-stream")

			records := make([]map[string]any, tt.recordCount)
			for i := range records {
				records[i] = map[string]any{"Data": "aGVsbG8="}
			}

			rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
				"DeliveryStreamName": "batch-id-stream",
				"Records":            records,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				RequestResponses []struct {
					RecordID     string `json:"RecordId"`
					ErrorCode    string `json:"ErrorCode"`
					ErrorMessage string `json:"ErrorMessage"`
				} `json:"RequestResponses"`
				FailedPutCount int `json:"FailedPutCount"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			assert.Equal(t, 0, out.FailedPutCount)
			require.Len(t, out.RequestResponses, tt.recordCount,
				"RequestResponses must have one entry per input record")

			seen := make(map[string]bool)
			for i, resp := range out.RequestResponses {
				assert.NotEmpty(t, resp.RecordID,
					"record %d must have a non-empty RecordId", i)
				assert.Empty(t, resp.ErrorCode,
					"successful record %d must have no ErrorCode", i)
				assert.False(t, seen[resp.RecordID],
					"RecordId must be unique across records; duplicate: %s", resp.RecordID)
				seen[resp.RecordID] = true
			}
		})
	}
}
