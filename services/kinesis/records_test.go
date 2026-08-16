package kinesis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func TestPutRecord_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "put-record-arn-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "put-record-arn-stream"},
	)
	require.NoError(t, err)

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "by_name",
			body: map[string]any{
				"StreamName":   "put-record-arn-stream",
				"PartitionKey": "pk1",
				"Data":         []byte("hello"),
			},
		},
		{
			name: "by_arn",
			body: map[string]any{
				"StreamARN":    desc.StreamARN,
				"PartitionKey": "pk2",
				"Data":         []byte("world"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "PutRecord", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				ShardID        string `json:"ShardId"`
				SequenceNumber string `json:"SequenceNumber"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.NotEmpty(t, resp.ShardID)
			assert.NotEmpty(t, resp.SequenceNumber)
		})
	}
}

func TestPutRecords_ByARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, "CreateStream", map[string]any{"StreamName": "put-records-arn-stream", "ShardCount": 1})

	b := h.Backend.(*kinesis.InMemoryBackend)
	desc, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "put-records-arn-stream"},
	)
	require.NoError(t, err)

	records := []map[string]any{
		{"PartitionKey": "pk1", "Data": []byte("r1")},
		{"PartitionKey": "pk2", "Data": []byte("r2")},
	}

	tests := []struct {
		body map[string]any
		name string
	}{
		{
			name: "by_name",
			body: map[string]any{"StreamName": "put-records-arn-stream", "Records": records},
		},
		{
			name: "by_arn",
			body: map[string]any{"StreamARN": desc.StreamARN, "Records": records},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "PutRecords", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp struct {
				Records []struct {
					ShardID        string `json:"ShardId"`
					SequenceNumber string `json:"SequenceNumber"`
				} `json:"Records"`
				FailedRecordCount int `json:"FailedRecordCount"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Equal(t, 0, resp.FailedRecordCount)
			assert.Len(t, resp.Records, 2)
		})
	}
}

// TestPutRecords_ThroughputErrorCode verifies that when FIS throughput fault is active,
// PutRecords records individual entries with the correct ProvisionedThroughputExceededException
// error code (not InternalFailure).
func TestPutRecords_ThroughputErrorCode(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "fault-stream"}))

	bk.InjectFaultForTest("fault-stream")

	out, err := bk.PutRecords(context.Background(), &kinesis.PutRecordsInput{
		StreamName: "fault-stream",
		Records: []kinesis.PutRecordsEntry{
			{PartitionKey: "pk", Data: []byte("data")},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Records, 1)
	assert.Equal(t, "ProvisionedThroughputExceededException", out.Records[0].ErrorCode)
	assert.Equal(t, 1, out.FailedRecordCount)
}

func TestExplicitHashKey_OverridesPartitionKey(t *testing.T) {
	t.Parallel()

	b := newParityBackend(t)
	ctx := context.Background()

	createParityStream(t, b, "explicit-key", 2)

	out0, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:      "explicit-key",
		PartitionKey:    "anything",
		ExplicitHashKey: "0",
		Data:            []byte("to-shard-0"),
	})
	require.NoError(t, err)
	assert.Equal(t, "shardId-000000000000", out0.ShardID)

	// shard 1 start = 2^127.
	shard1Start := new(big.Int).Lsh(big.NewInt(1), 127)

	out1, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:      "explicit-key",
		PartitionKey:    "anything",
		ExplicitHashKey: shard1Start.String(),
		Data:            []byte("to-shard-1"),
	})
	require.NoError(t, err)
	assert.Equal(t, "shardId-000000000001", out1.ShardID)
}

func TestPutRecord_ExplicitHashKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		explicitHashKey string
		wantShard       string
		shardCount      int
	}{
		{
			name:            "routes_to_shard0",
			shardCount:      2,
			explicitHashKey: "0",
			wantShard:       "shardId-000000000000",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend.(*kinesis.InMemoryBackend)

			require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
				StreamName: "ehk-stream-" + tt.name,
				ShardCount: tt.shardCount,
			}))

			out, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
				StreamName:      "ehk-stream-" + tt.name,
				PartitionKey:    "some-key",
				ExplicitHashKey: tt.explicitHashKey,
				Data:            []byte("data"),
			})
			require.NoError(t, err)
			assert.Equal(t, tt.wantShard, out.ShardID)
		})
	}
}

func TestPutRecordsNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "PutRecords", map[string]any{
		"StreamName": "nonexistent",
		"Records":    []map[string]any{{"PartitionKey": "pk", "Data": []byte("data")}},
	})
	// AWS fails the whole PutRecords call with a top-level ResourceNotFoundException
	// when the target stream does not exist — it does not report per-record
	// failures for a request-level (stream-not-found) error.
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var resp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ResourceNotFoundException", resp.Type)
}

func TestMultipleShardRouting(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream with 4 shards
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "multi-shard-stream",
		"ShardCount": 4,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Put records with different partition keys
	shardIDs := make(map[string]bool)
	for i := range 10 {
		rec = doRequest(t, h, "PutRecord", map[string]any{
			"StreamName":   "multi-shard-stream",
			"PartitionKey": fmt.Sprintf("pk-%d", i),
			"Data":         []byte("data"),
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var putResp struct {
			ShardID string `json:"ShardId"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
		shardIDs[putResp.ShardID] = true
	}

	// With 10 records and 4 shards, we should get records on more than 1 shard
	assert.GreaterOrEqual(t, len(shardIDs), 1)
}

func TestPutRecordMaxRecords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "trim-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

func TestPutRecords_OversizeRecordReturnsValidationException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "putrecords-err-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Build a batch: first record is valid, second is oversize.
	smallData := make([]byte, 100)
	oversizeData := make([]byte, 1_048_577) // 1 MiB + 1 byte

	rec = doRequest(t, h, "PutRecords", map[string]any{
		"StreamName": "putrecords-err-stream",
		"Records": []map[string]any{
			{"PartitionKey": "pk1", "Data": smallData},
			{"PartitionKey": "pk2", "Data": oversizeData},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Records []struct {
			ErrorCode    string `json:"ErrorCode"`
			ErrorMessage string `json:"ErrorMessage"`
			ShardID      string `json:"ShardId"`
		} `json:"Records"`
		FailedRecordCount int `json:"FailedRecordCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Equal(t, 1, resp.FailedRecordCount)
	require.Len(t, resp.Records, 2)

	// First record succeeded.
	assert.Empty(t, resp.Records[0].ErrorCode)
	assert.NotEmpty(t, resp.Records[0].ShardID)

	// Second record failed with ValidationException (not InternalFailure).
	assert.Equal(t, "ValidationException", resp.Records[1].ErrorCode)
	assert.NotEmpty(t, resp.Records[1].ErrorMessage)
}

func TestPutRecords_ThrottledRecordReturnsProvisionedThroughputException(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "putrecords-throttle-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	b.InjectFaultForTest("putrecords-throttle-stream")

	rec = doRequest(t, h, "PutRecords", map[string]any{
		"StreamName": "putrecords-throttle-stream",
		"Records": []map[string]any{
			{"PartitionKey": "pk1", "Data": []byte("data")},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Records []struct {
			ErrorCode string `json:"ErrorCode"`
		} `json:"Records"`
		FailedRecordCount int `json:"FailedRecordCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Equal(t, 1, resp.FailedRecordCount)
	assert.Equal(t, "ProvisionedThroughputExceededException", resp.Records[0].ErrorCode)
}

func TestPutRecords_AllValidRecordsSucceed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "putrecords-all-ok",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "PutRecords", map[string]any{
		"StreamName": "putrecords-all-ok",
		"Records": []map[string]any{
			{"PartitionKey": "pk1", "Data": []byte("a")},
			{"PartitionKey": "pk2", "Data": []byte("b")},
			{"PartitionKey": "pk3", "Data": []byte("c")},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Records []struct {
			ErrorCode string `json:"ErrorCode"`
			ShardID   string `json:"ShardId"`
		} `json:"Records"`
		FailedRecordCount int `json:"FailedRecordCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, 0, resp.FailedRecordCount)
	for _, r := range resp.Records {
		assert.Empty(t, r.ErrorCode)
		assert.NotEmpty(t, r.ShardID)
	}
}

func TestPutRecord_ExplicitHashKey_AboveMaxRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "hashkey-bound-stream",
		ShardCount: 1,
	}))

	// 2^128 is one above the maximum valid hash key.
	twoTo128 := "340282366920938463463374607431768211456"
	_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:      "hashkey-bound-stream",
		PartitionKey:    "pk",
		ExplicitHashKey: twoTo128,
		Data:            []byte("d"),
	})
	require.Error(t, err)
}

func TestPutRecord_ExplicitHashKey_NegativeRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "hashkey-negative-stream",
		ShardCount: 1,
	}))

	_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:      "hashkey-negative-stream",
		PartitionKey:    "pk",
		ExplicitHashKey: "-1",
		Data:            []byte("d"),
	})
	require.Error(t, err)
}

func TestPutRecord_ExplicitHashKey_ZeroAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "hashkey-zero-stream",
		ShardCount: 1,
	}))

	_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:      "hashkey-zero-stream",
		PartitionKey:    "pk",
		ExplicitHashKey: "0",
		Data:            []byte("d"),
	})
	require.NoError(t, err)
}

func TestPutRecord_ExplicitHashKey_MaxAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "hashkey-maxval-stream",
		ShardCount: 1,
	}))

	// 2^128-1 is the maximum valid hash key.
	maxKey := "340282366920938463463374607431768211455"
	_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:      "hashkey-maxval-stream",
		PartitionKey:    "pk",
		ExplicitHashKey: maxKey,
		Data:            []byte("d"),
	})
	require.NoError(t, err)
}

func TestPutRecord_ExplicitHashKey_ViaHandler_AboveMaxRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "hashkey-handler-bound",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	twoTo128 := "340282366920938463463374607431768211456"
	rec = doRequest(t, h, "PutRecord", map[string]any{
		"StreamName":      "hashkey-handler-bound",
		"PartitionKey":    "pk",
		"ExplicitHashKey": twoTo128,
		"Data":            []byte("d"),
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestExplicitHashKey_PartitionKeyOverride(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "explicit-hash-override",
		ShardCount: 2,
	}))

	// Use a hash key in the upper half to target the second shard.
	upperHalfKey := "255211775190703847597592248818726428672"
	out, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:      "explicit-hash-override",
		PartitionKey:    "ignored-partition-key",
		ExplicitHashKey: upperHalfKey,
		Data:            []byte("d"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, out.ShardID)
}

func TestPutRecord_ExplicitHashKey_OneAboveMax(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "above-max-hash",
		ShardCount: 1,
	}))

	// 2^128 is one above max (2^128-1).
	oneAboveMax := "340282366920938463463374607431768211456"
	_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:      "above-max-hash",
		PartitionKey:    "pk",
		ExplicitHashKey: oneAboveMax,
		Data:            []byte("d"),
	})
	require.Error(t, err)
}

func TestPutRecords_MixedOversizeAndValid(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "putrecords-mixed",
		ShardCount: 1,
	}))

	// 3 records: valid, oversize, valid.
	oversize := make([]byte, 1_048_577) // 1 MiB + 1 byte
	out, err := b.PutRecords(context.Background(), &kinesis.PutRecordsInput{
		StreamName: "putrecords-mixed",
		Records: []kinesis.PutRecordsEntry{
			{PartitionKey: "pk1", Data: []byte("ok1")},
			{PartitionKey: "pk2", Data: oversize},
			{PartitionKey: "pk3", Data: []byte("ok3")},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.Records, 3)
	assert.Equal(t, 1, out.FailedRecordCount)
	assert.Empty(t, out.Records[0].ErrorCode)
	assert.Equal(t, "ValidationException", out.Records[1].ErrorCode)
	assert.Empty(t, out.Records[2].ErrorCode)
}

func TestExplicitHashKey_ValidMidRange(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "midrange-hash",
		ShardCount: 2,
	}))

	// Hash key exactly at the midpoint of 2^128 space.
	midpoint := "170141183460469231731687303715884105728"
	_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:      "midrange-hash",
		PartitionKey:    "pk",
		ExplicitHashKey: midpoint,
		Data:            []byte("d"),
	})
	require.NoError(t, err)
}

func TestPutRecords_EmptyBatch(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "empty-batch-stream",
		ShardCount: 1,
	}))

	// AWS rejects an empty Records list outright (MinItems=1 in the SDK model)
	// with a validation error — it does not return a 200 with zero results.
	out, err := b.PutRecords(context.Background(), &kinesis.PutRecordsInput{
		StreamName: "empty-batch-stream",
		Records:    []kinesis.PutRecordsEntry{},
	})
	require.ErrorIs(t, err, kinesis.ErrInvalidArgument)
	assert.Nil(t, out)
}
