package firehose_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestFirehoseHandler_PutRecord(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *firehose.Handler)
		streamName string
		data       string
		wantCode   int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			data:       base64.StdEncoding.EncodeToString([]byte("hello world")),
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			data:       base64.StdEncoding.EncodeToString([]byte("hello")),
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "raw_data",
			streamName: "my-stream",
			data:       "not-base64!@#",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
				"DeliveryStreamName": tt.streamName,
				"Record":             map[string]string{"Data": tt.data},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestPutRecord_Success_ReturnsRecordId verifies PutRecord returns a non-empty RecordId.
func TestPutRecord_Success_ReturnsRecordId(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "put-stream")

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "put-stream",
		"Record":             map[string]any{"Data": "aGVsbG8="},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		RecordID string `json:"RecordId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.RecordID)
}

func TestPutRecord_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "no-such-stream",
		"Record":             map[string]any{"Data": "aGVsbG8="},
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestPutRecord_ReturnsNonEmptyRecordID verifies the RecordId field is present and not
// a stub value.
func TestPutRecord_ReturnsNonEmptyRecordID(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "rec-stream"})

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "rec-stream",
		"Record":             map[string]any{"Data": "aGVsbG8="},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RecordId")
	assert.NotContains(t, rec.Body.String(), "stub-record-id")
}

// TestPutRecord_EmptyData verifies empty record Data is rejected.
func TestPutRecord_EmptyData(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "empty-data-stream")

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "empty-data-stream",
		"Record":             map[string]any{"Data": ""},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestPutRecord_Rejected_On_KinesisSource verifies PutRecord is rejected on non-DirectPut
// streams.
func TestPutRecord_Rejected_On_KinesisSource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ks-put-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s",
			"RoleARN":          "arn:aws:iam::000000000000:role/r",
		},
	})

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "ks-put-stream",
		"Record":             map[string]any{"Data": "aGVsbG8="},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestKinesisStreamAsSource_PutRecordRejected verifies that PutRecord on a
// KinesisStreamAsSource stream returns InvalidArgumentException.
func TestKinesisStreamAsSource_PutRecordRejected(t *testing.T) {
	t.Parallel()

	h, _ := auditHandler(t)
	auditCreateStream(t, h, "kinesis-src-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/my-src",
			"RoleARN":          "arn:aws:iam::000000000000:role/firehose",
		},
	})

	rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
		"DeliveryStreamName": "kinesis-src-stream",
		"Record":             map[string]any{"Data": "aGVsbG8="},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	assert.Equal(t, "InvalidArgumentException", body["__type"])
}

func TestFirehoseHandler_PutRecordBatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, h *firehose.Handler)
		streamName string
		data       string
		wantCode   int
	}{
		{
			name:       "success",
			streamName: "my-stream",
			data:       base64.StdEncoding.EncodeToString([]byte("rec1")),
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
		{
			name:       "not_found",
			streamName: "nonexistent",
			data:       base64.StdEncoding.EncodeToString([]byte("a")),
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "raw_data",
			streamName: "my-stream",
			data:       "not-base64!@#",
			setup: func(t *testing.T, h *firehose.Handler) {
				t.Helper()
				doFirehoseRequest(t, h, "CreateDeliveryStream", map[string]any{"DeliveryStreamName": "my-stream"})
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestFirehoseHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
				"DeliveryStreamName": tt.streamName,
				"Records":            []map[string]string{{"Data": tt.data}},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestPutRecordBatch_Success_ResponseShape verifies the response shape: RequestResponses
// non-nil and FailedPutCount 0.
func TestPutRecordBatch_Success_ResponseShape(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "batch-stream")

	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "batch-stream",
		"Records": []map[string]any{
			{"Data": "aGVsbG8="},
			{"Data": "d29ybGQ="},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		RequestResponses []any `json:"RequestResponses"`
		FailedPutCount   int   `json:"FailedPutCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, 0, out.FailedPutCount)
	assert.NotNil(t, out.RequestResponses)
}

// TestPutRecordBatch_ResponseShape verifies that each record gets a RecordId and
// FailedPutCount is always 0.
func TestPutRecordBatch_ResponseShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		recordData []string
	}{
		{
			name:       "single_record",
			recordData: []string{"aGVsbG8="},
		},
		{
			name: "five_records",
			recordData: []string{
				base64.StdEncoding.EncodeToString([]byte("rec-1")),
				base64.StdEncoding.EncodeToString([]byte("rec-2")),
				base64.StdEncoding.EncodeToString([]byte("rec-3")),
				base64.StdEncoding.EncodeToString([]byte("rec-4")),
				base64.StdEncoding.EncodeToString([]byte("rec-5")),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := auditHandler(t)
			auditCreateStream(t, h, "batch-resp-stream", nil)

			records := make([]map[string]any, len(tc.recordData))
			for i, d := range tc.recordData {
				records[i] = map[string]any{"Data": d}
			}

			rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
				"DeliveryStreamName": "batch-resp-stream",
				"Records":            records,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				RequestResponses []struct {
					RecordID string `json:"RecordId"`
				} `json:"RequestResponses"`
				FailedPutCount int `json:"FailedPutCount"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			assert.Equal(t, 0, out.FailedPutCount, "FailedPutCount must always be 0")
			assert.Len(t, out.RequestResponses, len(tc.recordData), "one response per record")
			for i, resp := range out.RequestResponses {
				assert.NotEmpty(t, resp.RecordID, "RecordId must be non-empty for record %d", i)
			}
		})
	}
}

// TestPutRecordBatch_PerRecordRecordId verifies that PutRecordBatch returns a
// RequestResponses array with one entry per input record, each carrying a unique
// non-empty RecordId.
func TestPutRecordBatch_PerRecordRecordId(t *testing.T) {
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

// TestPutRecordBatch_EmptyData verifies a batch containing an empty-Data record is
// rejected.
func TestPutRecordBatch_EmptyData(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "empty-batch-stream")

	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "empty-batch-stream",
		"Records":            []map[string]any{{"Data": ""}, {"Data": "aGVsbG8="}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestPutRecordBatch_Rejected_On_KinesisSource verifies PutRecordBatch is rejected on
// non-DirectPut streams.
func TestPutRecordBatch_Rejected_On_KinesisSource(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "ks-batch-stream", map[string]any{
		"DeliveryStreamType": "KinesisStreamAsSource",
		"KinesisStreamSourceConfiguration": map[string]any{
			"KinesisStreamARN": "arn:aws:kinesis:us-east-1:000000000000:stream/s",
			"RoleARN":          "arn:aws:iam::000000000000:role/r",
		},
	})

	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "ks-batch-stream",
		"Records":            []map[string]any{{"Data": "aGVsbG8="}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
