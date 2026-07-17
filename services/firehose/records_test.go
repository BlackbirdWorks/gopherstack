package firehose_test

import (
	"context"
	"encoding/base64"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/firehose"
)

func TestPutRecord(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "my-stream"})

	err := b.PutRecord(context.TODO(), "my-stream", []byte("hello world"))
	require.NoError(t, err)
}

// TestPutRecord_EmptyRecordRejected verifies that AWS rejects empty record Data
// with InvalidArgumentException (AWS accuracy: issue #34).
func TestPutRecord_EmptyRecordRejected(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "empty-rec-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::empty-bucket",
		},
	})
	require.NoError(t, err)

	// AWS rejects empty records at ingestion.
	err = b.PutRecord(context.TODO(), "empty-rec-stream", []byte{})
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrValidation)
}

// TestPutRecord_RecordTooLarge verifies that records exceeding the 1,000 KB limit
// are rejected with ErrRecordTooLarge.
func TestPutRecord_RecordTooLarge(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "limit-stream"})
	require.NoError(t, err)

	oversized := make([]byte, 1_001*1024) // 1,001 KB — one byte over the limit
	putErr := b.PutRecord(context.TODO(), "limit-stream", oversized)
	require.Error(t, putErr)
	assert.ErrorIs(t, putErr, firehose.ErrRecordTooLarge)
}

// TestPutRecord_FlushSnapshotUnderLock verifies that after a size-based flush the
// buffer is reset atomically: a subsequent PutRecord starts with a zeroed counter and
// the old records are not double-delivered.
func TestPutRecord_FlushSnapshotUnderLock(t *testing.T) {
	t.Parallel()

	s3mock := &mockS3Storer{}
	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	b.SetS3Backend(s3mock)

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "atomic-flush-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::atomic-bucket",
			BufferingHints: &firehose.BufferingHints{
				SizeInMBs:         1,
				IntervalInSeconds: 300,
			},
		},
	})
	require.NoError(t, err)

	// Two records of 512 KB each sum to 1 MB and trigger one size-based flush.
	require.NoError(t, b.PutRecord(context.TODO(), "atomic-flush-stream", make([]byte, 512*1024)))
	require.NoError(t, b.PutRecord(context.TODO(), "atomic-flush-stream", make([]byte, 512*1024)))

	// After the flush, the buffer is zeroed; a small subsequent record should not
	// trigger another flush automatically.
	require.NoError(t, b.PutRecord(context.TODO(), "atomic-flush-stream", []byte("small")))

	// Only one S3 delivery should have occurred (from the over-limit puts).
	assert.Len(t, s3mock.calls, 1)
}

// TestErrValidation_Exported verifies the ErrValidation sentinel is reachable and
// wraps PutRecord's empty-data rejection.
func TestErrValidation_Exported(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "s"})
	require.NoError(t, err)

	err = b.PutRecord(context.TODO(), "s", []byte{})
	require.Error(t, err)
	assert.ErrorIs(t, err, firehose.ErrValidation)
}

func TestPutRecordBatch(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, _ = b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "my-stream"})

	failed, err := b.PutRecordBatch(context.TODO(), "my-stream", [][]byte{[]byte("a"), []byte("b")})
	require.NoError(t, err)
	assert.Equal(t, 0, failed)
}

// TestPutRecordBatch_AllEmptyRecordsRejected verifies PutRecordBatch rejects a batch
// where any record has empty Data (AWS accuracy: issue #34).
func TestPutRecordBatch_AllEmptyRecordsRejected(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{
		Name: "all-empty-stream",
		S3Destination: &firehose.S3DestinationDescription{
			BucketARN: "arn:aws:s3:::empty-bucket",
		},
	})
	require.NoError(t, err)

	// Batch with empty records is rejected.
	_, batchErr := b.PutRecordBatch(context.TODO(), "all-empty-stream", [][]byte{{}, {}})
	require.Error(t, batchErr)
	assert.ErrorIs(t, batchErr, firehose.ErrValidation)
}

// TestPutRecordBatch_TooManyRecords verifies that a batch exceeding 500 records
// is rejected with ErrBatchTooLarge.
func TestPutRecordBatch_TooManyRecords(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "batch-limit-stream"})
	require.NoError(t, err)

	records := make([][]byte, 501)
	for i := range records {
		records[i] = []byte("x")
	}

	_, putErr := b.PutRecordBatch(context.TODO(), "batch-limit-stream", records)
	require.Error(t, putErr)
	assert.ErrorIs(t, putErr, firehose.ErrBatchTooLarge)
}

// TestPutRecordBatch_RecordInBatchTooLarge verifies that individual records within a
// batch exceeding the 1,000 KB limit are also rejected with ErrRecordTooLarge.
func TestPutRecordBatch_RecordInBatchTooLarge(t *testing.T) {
	t.Parallel()

	b := firehose.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDeliveryStream(context.TODO(), firehose.CreateDeliveryStreamInput{Name: "batch-rec-limit-stream"})
	require.NoError(t, err)

	records := [][]byte{
		[]byte("ok"),
		make([]byte, 1_001*1024), // oversized second record
	}

	_, putErr := b.PutRecordBatch(context.TODO(), "batch-rec-limit-stream", records)
	require.Error(t, putErr)
	assert.ErrorIs(t, putErr, firehose.ErrRecordTooLarge)
}

// TestPutRecord_RecordSizeBounds verifies (via the handler) that PutRecord rejects
// records larger than 1,000 KB (1,024,000 bytes) and accepts records at or below the
// limit. Real AWS Firehose returns InvalidArgumentException for oversized records.
func TestPutRecord_RecordSizeBounds(t *testing.T) {
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

// TestPutRecordBatch_RecordCountBounds verifies (via the handler) that PutRecordBatch
// rejects batches with more than 500 records. Real AWS Firehose returns
// InvalidArgumentException for batches exceeding this limit.
func TestPutRecordBatch_RecordCountBounds(t *testing.T) {
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

// TestPutRecordBatch_OversizedRecordInBatch verifies (via the handler) that
// PutRecordBatch rejects batches containing individual records larger than 1,000 KB.
// Real AWS Firehose rejects the entire batch, not just the oversized record.
func TestPutRecordBatch_OversizedRecordInBatch(t *testing.T) {
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
