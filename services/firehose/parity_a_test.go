package firehose_test

import (
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParity_PutRecord_RecordSizeBounds verifies that PutRecord rejects records
// larger than 1,000 KB (1,024,000 bytes) and accepts records at or below the limit.
// Real AWS Firehose returns InvalidArgumentException for oversized records.
func TestParity_PutRecord_RecordSizeBounds(t *testing.T) {
	t.Parallel()

	const maxBytes = 1_000 * 1024

	tests := []struct {
		name     string
		dataSize int
		wantCode int
	}{
		{
			name:     "one_byte_accepted",
			dataSize: 1,
			wantCode: http.StatusOK,
		},
		{
			name:     "at_limit_accepted",
			dataSize: maxBytes,
			wantCode: http.StatusOK,
		},
		{
			name:     "one_over_limit_rejected",
			dataSize: maxBytes + 1,
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "two_mb_rejected",
			dataSize: 2 * 1024 * 1024,
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)
			createStream(t, h, "test-stream")

			data := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("x", tt.dataSize)))

			rec := doFirehoseRequest(t, h, "PutRecord", map[string]any{
				"DeliveryStreamName": "test-stream",
				"Record":             map[string]any{"Data": data},
			})

			assert.Equal(t, tt.wantCode, rec.Code, "dataSize=%d", tt.dataSize)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidArgumentException",
					"expected InvalidArgumentException for oversized record")
			}
		})
	}
}

// TestParity_PutRecordBatch_RecordCountBounds verifies that PutRecordBatch rejects
// batches with more than 500 records. Real AWS Firehose returns InvalidArgumentException
// for batches exceeding this limit.
func TestParity_PutRecordBatch_RecordCountBounds(t *testing.T) {
	t.Parallel()

	makeRecords := func(n int) []map[string]any {
		records := make([]map[string]any, n)
		data := base64.StdEncoding.EncodeToString([]byte("x"))
		for i := range records {
			records[i] = map[string]any{"Data": data}
		}

		return records
	}

	tests := []struct {
		name        string
		recordCount int
		wantCode    int
	}{
		{
			name:        "one_record_accepted",
			recordCount: 1,
			wantCode:    http.StatusOK,
		},
		{
			name:        "at_limit_accepted",
			recordCount: 500,
			wantCode:    http.StatusOK,
		},
		{
			name:        "one_over_limit_rejected",
			recordCount: 501,
			wantCode:    http.StatusBadRequest,
		},
		{
			name:        "one_thousand_rejected",
			recordCount: 1000,
			wantCode:    http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestFirehoseHandler(t)
			createStream(t, h, "batch-stream")

			rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
				"DeliveryStreamName": "batch-stream",
				"Records":            makeRecords(tt.recordCount),
			})

			assert.Equal(t, tt.wantCode, rec.Code, "recordCount=%d", tt.recordCount)

			if tt.wantCode == http.StatusBadRequest {
				assert.Contains(t, rec.Body.String(), "InvalidArgumentException",
					"expected InvalidArgumentException for oversized batch")
			}
		})
	}
}

// TestParity_PutRecordBatch_OversizedRecordInBatch verifies that PutRecordBatch
// rejects batches containing individual records larger than 1,000 KB.
// Real AWS Firehose rejects the entire batch, not just the oversized record.
func TestParity_PutRecordBatch_OversizedRecordInBatch(t *testing.T) {
	t.Parallel()

	h := newTestFirehoseHandler(t)
	createStream(t, h, "batch-stream-large")

	const maxBytes = 1_000 * 1024
	oversizedData := base64.StdEncoding.EncodeToString([]byte(strings.Repeat("y", maxBytes+1)))
	smallData := base64.StdEncoding.EncodeToString([]byte("small"))

	rec := doFirehoseRequest(t, h, "PutRecordBatch", map[string]any{
		"DeliveryStreamName": "batch-stream-large",
		"Records": []map[string]any{
			{"Data": smallData},
			{"Data": oversizedData},
		},
	})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidArgumentException")
}
