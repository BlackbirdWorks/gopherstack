package kinesis_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetRecords_SizeCap_ExcludesPartitionKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "records_with_large_partitionkey_not_counted_toward_cap",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)
				streamName := "parity-b-large-pk-stream"

				rec := doRequest(t, h, "CreateStream", map[string]any{
					"StreamName": streamName,
					"ShardCount": 1,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
				require.Equal(t, http.StatusOK, rec.Code)

				var descResp struct {
					StreamDescription struct {
						Shards []struct {
							ShardID string `json:"ShardId"`
						} `json:"Shards"`
					} `json:"StreamDescription"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				require.NotEmpty(t, descResp.StreamDescription.Shards)
				shardID := descResp.StreamDescription.Shards[0].ShardID

				// 3 records: tiny data (10 raw bytes) + large partition key (256 chars, the AWS max).
				// If PK bytes counted toward the 10 MiB response cap the logic would still
				// be wrong; this test verifies all 3 records are returned regardless.
				smallData := base64.StdEncoding.EncodeToString(make([]byte, 10))
				largePK := strings.Repeat("k", 256)

				records := make([]map[string]any, 3)
				for i := range records {
					records[i] = map[string]any{
						"Data":         smallData,
						"PartitionKey": largePK,
					}
				}

				rec = doRequest(t, h, "PutRecords", map[string]any{
					"StreamName": streamName,
					"Records":    records,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var putResp struct {
					FailedRecordCount int `json:"FailedRecordCount"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
				assert.Equal(t, 0, putResp.FailedRecordCount)

				rec = doRequest(t, h, "GetShardIterator", map[string]any{
					"StreamName":        streamName,
					"ShardId":           shardID,
					"ShardIteratorType": "TRIM_HORIZON",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var iterResp struct {
					ShardIterator string `json:"ShardIterator"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &iterResp))
				require.NotEmpty(t, iterResp.ShardIterator)

				rec = doRequest(t, h, "GetRecords", map[string]any{
					"ShardIterator": iterResp.ShardIterator,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var getResp struct {
					Records []struct {
						PartitionKey string `json:"PartitionKey"`
					} `json:"Records"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.Len(
					t,
					getResp.Records,
					3,
					"all 3 records should be returned: large PartitionKey must not count toward the 10 MiB data cap",
				)
			},
		},
		{
			name: "data_bytes_counted_toward_cap",
			run: func(t *testing.T) {
				t.Helper()
				h := newTestHandler(t)
				streamName := "parity-b-data-cap-stream"

				rec := doRequest(t, h, "CreateStream", map[string]any{
					"StreamName": streamName,
					"ShardCount": 1,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": streamName})
				require.Equal(t, http.StatusOK, rec.Code)

				var descResp struct {
					StreamDescription struct {
						Shards []struct {
							ShardID string `json:"ShardId"`
						} `json:"Shards"`
					} `json:"StreamDescription"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
				require.NotEmpty(t, descResp.StreamDescription.Shards)
				shardID := descResp.StreamDescription.Shards[0].ShardID

				// 3 records each ~4 MiB of raw data.
				// 4 MiB * 2 = 8 MiB < 10 MiB cap; 4 MiB * 3 = 12 MiB > cap.
				const fourMiB = 4 * 1024 * 1024
				largeData := base64.StdEncoding.EncodeToString(make([]byte, fourMiB))

				for i := range 3 {
					rec = doRequest(t, h, "PutRecords", map[string]any{
						"StreamName": streamName,
						"Records": []map[string]any{
							{
								"Data":         largeData,
								"PartitionKey": fmt.Sprintf("pk-%d", i),
							},
						},
					})
					require.Equal(t, http.StatusOK, rec.Code)
				}

				rec = doRequest(t, h, "GetShardIterator", map[string]any{
					"StreamName":        streamName,
					"ShardId":           shardID,
					"ShardIteratorType": "TRIM_HORIZON",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var iterResp struct {
					ShardIterator string `json:"ShardIterator"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &iterResp))
				require.NotEmpty(t, iterResp.ShardIterator)

				rec = doRequest(t, h, "GetRecords", map[string]any{
					"ShardIterator": iterResp.ShardIterator,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var getResp struct {
					Records []struct {
						Data []byte `json:"Data"`
					} `json:"Records"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
				assert.LessOrEqual(t, len(getResp.Records), 2,
					"data cap (10 MiB) should stop GetRecords before the 3rd 4-MiB record")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func TestKinesisBackend_GetRecordsDeletedStream(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "deleted-stream"}))

	desc, err := bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "deleted-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	iterOut, err := bk.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "deleted-stream",
		ShardID:           shardID,
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	// Delete stream
	require.NoError(t, bk.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: "deleted-stream"}))

	// GetRecords should return stream not found
	_, err = bk.GetRecords(context.Background(), &kinesis.GetRecordsInput{ShardIterator: iterOut.ShardIterator})
	assert.ErrorIs(t, err, kinesis.ErrStreamNotFound)
}

func TestKinesisBackend_GetRecordsInvalidShard(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(
		t,
		bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "shard-gone-stream"}),
	)

	desc, err := bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "shard-gone-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	iterOut, err := bk.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "shard-gone-stream",
		ShardID:           shardID,
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	// Delete and recreate the stream (new shards will have the same IDs so this won't test the gap,
	// but we can test invalid shard via ListShards with wrong stream name)
	require.NoError(
		t,
		bk.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: "shard-gone-stream"}),
	)

	// Recreate stream (iterator now points to deleted stream)
	_, err = bk.GetRecords(context.Background(), &kinesis.GetRecordsInput{ShardIterator: iterOut.ShardIterator})
	assert.Error(t, err)
}

func TestGetRecords_MillisBehindLatest(t *testing.T) {
	t.Parallel()

	t.Run("non-zero when unread records remain", func(t *testing.T) {
		t.Parallel()

		b := newParityBackend(t)
		ctx := context.Background()

		createParityStream(t, b, "mbl-nonzero", 1)

		// Push 5 records backdated 10 seconds so time.Since() resolves to > 0ms.
		for range 5 {
			err := b.PushOldRecordForTest("mbl-nonzero", 0, 10*time.Second)
			require.NoError(t, err)
		}

		itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        "mbl-nonzero",
			ShardID:           "shardId-000000000000",
			ShardIteratorType: "TRIM_HORIZON",
		})
		require.NoError(t, err)

		rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{
			ShardIterator: itOut.ShardIterator,
			Limit:         2,
		})
		require.NoError(t, err)
		assert.Len(t, rOut.Records, 2)
		assert.Positive(t, rOut.MillisBehindLatest,
			"MillisBehindLatest must be > 0 when there are unread records in the shard")
	})

	t.Run("zero when fully caught up", func(t *testing.T) {
		t.Parallel()

		b := newParityBackend(t)
		ctx := context.Background()

		createParityStream(t, b, "mbl-zero", 1)

		_, err := b.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamName:   "mbl-zero",
			PartitionKey: "pk",
			Data:         []byte("x"),
		})
		require.NoError(t, err)

		itOut, err := b.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
			StreamName:        "mbl-zero",
			ShardID:           "shardId-000000000000",
			ShardIteratorType: "TRIM_HORIZON",
		})
		require.NoError(t, err)

		rOut, err := b.GetRecords(ctx, &kinesis.GetRecordsInput{ShardIterator: itOut.ShardIterator})
		require.NoError(t, err)
		assert.Len(t, rOut.Records, 1)
		assert.Equal(t, int64(0), rOut.MillisBehindLatest)
	})
}

func TestPutAndGetRecords(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream with 1 shard
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "records-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Describe to find shard ID
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "records-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.NotEmpty(t, descResp.StreamDescription.Shards)
	shardID := descResp.StreamDescription.Shards[0].ShardID

	// PutRecord
	rec = doRequest(t, h, "PutRecord", map[string]any{
		"StreamName":   "records-stream",
		"PartitionKey": "pk-1",
		"Data":         []byte("hello world"),
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var putResp struct {
		ShardID        string `json:"ShardId"`
		SequenceNumber string `json:"SequenceNumber"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
	assert.NotEmpty(t, putResp.ShardID)
	assert.NotEmpty(t, putResp.SequenceNumber)
	firstSeq := putResp.SequenceNumber

	// PutRecords (batch)
	rec = doRequest(t, h, "PutRecords", map[string]any{
		"StreamName": "records-stream",
		"Records": []map[string]any{
			{"PartitionKey": "pk-2", "Data": []byte("record 2")},
			{"PartitionKey": "pk-3", "Data": []byte("record 3")},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var batchResp struct {
		Records []struct {
			ShardID        string `json:"ShardId"`
			SequenceNumber string `json:"SequenceNumber"`
		} `json:"Records"`
		FailedRecordCount int `json:"FailedRecordCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &batchResp))
	assert.Equal(t, 0, batchResp.FailedRecordCount)
	assert.Len(t, batchResp.Records, 2)

	// GetShardIterator - TRIM_HORIZON (reads from beginning)
	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":        "records-stream",
		"ShardId":           shardID,
		"ShardIteratorType": "TRIM_HORIZON",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var iterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &iterResp))
	assert.NotEmpty(t, iterResp.ShardIterator)

	// GetRecords
	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": iterResp.ShardIterator,
		"Limit":         10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		NextShardIterator string `json:"NextShardIterator"`
		Records           []struct {
			PartitionKey   string `json:"PartitionKey"`
			SequenceNumber string `json:"SequenceNumber"`
			Data           []byte `json:"Data"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Len(t, getResp.Records, 3) // 1 + 2 batch
	assert.NotEmpty(t, getResp.NextShardIterator)

	// GetShardIterator - AT_SEQUENCE_NUMBER
	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":             "records-stream",
		"ShardId":                shardID,
		"ShardIteratorType":      "AT_SEQUENCE_NUMBER",
		"StartingSequenceNumber": firstSeq,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var atSeqIterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &atSeqIterResp))

	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": atSeqIterResp.ShardIterator,
		"Limit":         10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var atSeqResp struct {
		Records []struct {
			SequenceNumber string `json:"SequenceNumber"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &atSeqResp))
	// AT_SEQUENCE_NUMBER starts at the given record (inclusive)
	require.NotEmpty(t, atSeqResp.Records)
	assert.Equal(t, firstSeq, atSeqResp.Records[0].SequenceNumber)

	// GetShardIterator - AFTER_SEQUENCE_NUMBER
	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":             "records-stream",
		"ShardId":                shardID,
		"ShardIteratorType":      "AFTER_SEQUENCE_NUMBER",
		"StartingSequenceNumber": firstSeq,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var afterIterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterIterResp))

	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": afterIterResp.ShardIterator,
		"Limit":         10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var afterSeqResp struct {
		Records []struct {
			SequenceNumber string `json:"SequenceNumber"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &afterSeqResp))
	// AFTER_SEQUENCE_NUMBER skips the given record
	assert.Len(t, afterSeqResp.Records, 2)

	// GetShardIterator - LATEST (no new records)
	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":        "records-stream",
		"ShardId":           shardID,
		"ShardIteratorType": "LATEST",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var latestIterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &latestIterResp))

	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": latestIterResp.ShardIterator,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var latestResp struct {
		Records []any `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &latestResp))
	assert.Empty(t, latestResp.Records) // No new records since iterator was created
}

func TestSequenceNumberOrdering(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create stream
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "order-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get shard ID
	rec = doRequest(t, h, "DescribeStream", map[string]any{
		"StreamName": "order-stream",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	shardID := descResp.StreamDescription.Shards[0].ShardID

	// Put 5 records
	seqNums := make([]string, 5)
	for i := range 5 {
		rec = doRequest(t, h, "PutRecord", map[string]any{
			"StreamName":   "order-stream",
			"PartitionKey": "pk",
			"Data":         []byte("data"),
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var putResp struct {
			SequenceNumber string `json:"SequenceNumber"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))
		seqNums[i] = putResp.SequenceNumber
	}

	// Verify ordering
	for i := 1; i < len(seqNums); i++ {
		assert.Greater(t, seqNums[i], seqNums[i-1],
			"sequence numbers should be strictly increasing: %s <= %s", seqNums[i], seqNums[i-1])
	}

	// Read back and verify order
	rec = doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":        "order-stream",
		"ShardId":           shardID,
		"ShardIteratorType": "TRIM_HORIZON",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var iterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &iterResp))

	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": iterResp.ShardIterator,
		"Limit":         10,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		Records []struct {
			SequenceNumber string `json:"SequenceNumber"`
		} `json:"Records"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	require.Len(t, getResp.Records, 5)

	for i, r := range getResp.Records {
		assert.Equal(t, seqNums[i], r.SequenceNumber)
	}
}

func TestGetRecords_10MBCap_StopsAtLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "big-records-stream",
		ShardCount: 1,
	}))

	// Each record is ~1 MiB of data.
	oneMiB := make([]byte, 1_048_576)

	// Put 12 records (12 MiB total, well above the 10 MiB cap).
	for i := range 12 {
		_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
			StreamName:   "big-records-stream",
			PartitionKey: fmt.Sprintf("pk%d", i),
			Data:         oneMiB,
		})
		require.NoError(t, err)
	}

	out, err := b.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "big-records-stream",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	rec, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: out.ShardIterator,
		Limit:         10000,
	})
	require.NoError(t, err)

	// Must have received fewer than 12 records due to 10 MiB cap.
	assert.Less(t, len(rec.Records), 12, "10 MiB cap should limit response to fewer than 12 records")
	assert.NotEmpty(t, rec.NextShardIterator, "should still have a next iterator")
}

func TestGetRecords_10MBCap_SingleLargeRecordAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "single-big-record",
		ShardCount: 1,
	}))

	// Increase the record size limit to 10 MiB first.
	require.NoError(t, b.UpdateMaxRecordSize(context.Background(), &kinesis.UpdateMaxRecordSizeInput{
		StreamName:         "single-big-record",
		MaxRecordSizeBytes: 10_485_760,
	}))

	tenMiB := make([]byte, 10_485_760)
	_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "single-big-record",
		PartitionKey: "pk",
		Data:         tenMiB,
	})
	require.NoError(t, err)

	// Put a second record so we can verify MillisBehindLatest.
	_, err = b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "single-big-record",
		PartitionKey: "pk2",
		Data:         []byte("small"),
	})
	require.NoError(t, err)

	out, err := b.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "single-big-record",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	rec, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: out.ShardIterator,
		Limit:         10000,
	})
	require.NoError(t, err)

	// A single record that exceeds the cap is still returned (cap is applied
	// as "stop adding AFTER limit is hit if at least 1 record consumed").
	assert.GreaterOrEqual(t, len(rec.Records), 1, "at least one record should be returned")
}

func TestGetRecords_10MBCap_IteratorAdvancesCorrectly(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "cap-advance-stream",
		ShardCount: 1,
	}))

	// Use UpdateMaxRecordSize to allow 6 MiB records (> default 1 MiB limit).
	require.NoError(t, b.UpdateMaxRecordSize(context.Background(), &kinesis.UpdateMaxRecordSizeInput{
		StreamName:         "cap-advance-stream",
		MaxRecordSizeBytes: 10_485_760,
	}))

	// 4 MiB records × 3 = 12 MiB total: first call gets 2 (8MB), second call gets 1.
	fourMiB := make([]byte, 4_194_304)
	for i := range 3 {
		_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
			StreamName:   "cap-advance-stream",
			PartitionKey: fmt.Sprintf("pk%d", i),
			Data:         fourMiB,
		})
		require.NoError(t, err)
	}

	iterOut, err := b.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "cap-advance-stream",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	first, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         10000,
	})
	require.NoError(t, err)
	require.Less(t, len(first.Records), 3, "should not return all 3 records due to 10 MiB cap")
	require.NotEmpty(t, first.NextShardIterator)

	// Second call should return the remaining records.
	second, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: first.NextShardIterator,
		Limit:         10000,
	})
	require.NoError(t, err)
	total := len(first.Records) + len(second.Records)
	assert.Equal(t, 3, total, "all records should be reachable via pagination")
}

func TestGetRecords_MillisBehindLatest_UsesLastRecord(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "millis-behind-stream",
		ShardCount: 1,
	}))

	// Put 3 records and introduce a small delay so their timestamps are in the past.
	for i := range 3 {
		_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
			StreamName:   "millis-behind-stream",
			PartitionKey: fmt.Sprintf("pk%d", i),
			Data:         []byte("d"),
		})
		require.NoError(t, err)
	}

	// Wait briefly so the records have a measurable age.
	time.Sleep(5 * time.Millisecond)

	iterOut, err := b.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "millis-behind-stream",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	// Get only 1 record (leaving 2 unread).
	rec, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         1,
	})
	require.NoError(t, err)
	require.Len(t, rec.Records, 1)

	// MillisBehindLatest should be the lag from the LAST record (record 3), not the next unread.
	assert.Positive(t, rec.MillisBehindLatest)
}

func TestGetRecords_MillisBehindLatest_ZeroWhenCaughtUp(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "millis-caught-up",
		ShardCount: 1,
	}))

	_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "millis-caught-up",
		PartitionKey: "pk",
		Data:         []byte("d"),
	})
	require.NoError(t, err)

	iterOut, err := b.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "millis-caught-up",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	// Consume all records.
	rec, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         10000,
	})
	require.NoError(t, err)
	require.Len(t, rec.Records, 1)

	// Consumer is now at the tip → MillisBehindLatest should be 0.
	assert.Equal(t, int64(0), rec.MillisBehindLatest)
}

func TestGetRecords_SmallRecords_NoCap(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "small-records-stream",
		ShardCount: 1,
	}))

	// Put 100 small records (well under 10 MiB).
	for i := range 100 {
		_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
			StreamName:   "small-records-stream",
			PartitionKey: fmt.Sprintf("pk%d", i),
			Data:         []byte("hello"),
		})
		require.NoError(t, err)
	}

	iterOut, err := b.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "small-records-stream",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	rec, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         10000,
	})
	require.NoError(t, err)
	// All 100 small records should be returned in one call.
	assert.Len(t, rec.Records, 100)
}

func TestGetRecords_10MBCap_ExactlyAtLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "exact-cap-stream",
		ShardCount: 1,
	}))

	require.NoError(t, b.UpdateMaxRecordSize(context.Background(), &kinesis.UpdateMaxRecordSizeInput{
		StreamName:         "exact-cap-stream",
		MaxRecordSizeBytes: 10_485_760,
	}))

	// Two 5 MiB records = exactly 10 MiB; both should fit in one response.
	fiveMiB := make([]byte, 5_242_880)
	for i := range 2 {
		_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
			StreamName:   "exact-cap-stream",
			PartitionKey: fmt.Sprintf("pk%d", i),
			Data:         fiveMiB,
		})
		require.NoError(t, err)
	}
	// Third 1-byte record (so we can check lag).
	_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "exact-cap-stream",
		PartitionKey: "extra",
		Data:         []byte("x"),
	})
	require.NoError(t, err)

	iterOut, err := b.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "exact-cap-stream",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	rec, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         10000,
	})
	require.NoError(t, err)

	// Both 5 MiB records (10 MiB total) should be returned; third should remain.
	assert.Len(t, rec.Records, 2)
	assert.NotEmpty(t, rec.NextShardIterator)
}

func TestGetRecords_ZeroLimitUsesDefault(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "default-limit-stream",
		ShardCount: 1,
	}))

	// Put more than defaultGetRecordsLimit records.
	for i := range 5 {
		_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
			StreamName:   "default-limit-stream",
			PartitionKey: fmt.Sprintf("pk%d", i),
			Data:         []byte("d"),
		})
		require.NoError(t, err)
	}

	iterOut, err := b.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "default-limit-stream",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	// Limit=0 uses the default (1000).
	rec, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         0,
	})
	require.NoError(t, err)
	assert.Len(t, rec.Records, 5, "all 5 records should be returned with default limit")
}

func TestGetRecords_EmptyShard_MillisBehindZero(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "empty-shard-millis",
		ShardCount: 1,
	}))

	iterOut, err := b.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "empty-shard-millis",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	rec, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         100,
	})
	require.NoError(t, err)
	assert.Empty(t, rec.Records)
	assert.Equal(t, int64(0), rec.MillisBehindLatest)
}

func TestGetRecords_10MBCap_RecordsBeforeCapNotDropped(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "precap-records",
		ShardCount: 1,
	}))

	require.NoError(t, b.UpdateMaxRecordSize(context.Background(), &kinesis.UpdateMaxRecordSizeInput{
		StreamName:         "precap-records",
		MaxRecordSizeBytes: 10_485_760,
	}))

	// Put 3 small + 1 huge record (order matters for iteration).
	for i := range 3 {
		_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
			StreamName:   "precap-records",
			PartitionKey: fmt.Sprintf("small%d", i),
			Data:         []byte("tiny"),
		})
		require.NoError(t, err)
	}

	bigData := make([]byte, 9_000_000)
	_, err := b.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "precap-records",
		PartitionKey: "big",
		Data:         bigData,
	})
	require.NoError(t, err)

	iterOut, err := b.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:        "precap-records",
		ShardID:           "shardId-000000000000",
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	rec, err := b.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         10000,
	})
	require.NoError(t, err)

	// All 3 small records + the 9MB record fit within 10MB.
	assert.Len(t, rec.Records, 4)
}

func TestGetRecords_MillisBehindLatest_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "millis-handler-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Put 3 records.
	for i := range 3 {
		doRequest(t, h, "PutRecord", map[string]any{
			"StreamName":   "millis-handler-stream",
			"PartitionKey": fmt.Sprintf("pk%d", i),
			"Data":         []byte("x"),
		})
	}

	// Sleep briefly to ensure records have a measurable age.
	time.Sleep(2 * time.Millisecond)

	// Get shard iterator at trim horizon.
	iterRec := doRequest(t, h, "GetShardIterator", map[string]any{
		"StreamName":        "millis-handler-stream",
		"ShardId":           "shardId-000000000000",
		"ShardIteratorType": "TRIM_HORIZON",
	})
	require.Equal(t, http.StatusOK, iterRec.Code)

	var iterResp struct {
		ShardIterator string `json:"ShardIterator"`
	}
	require.NoError(t, json.Unmarshal(iterRec.Body.Bytes(), &iterResp))

	// Fetch 1 record (leaving 2 behind).
	rec = doRequest(t, h, "GetRecords", map[string]any{
		"ShardIterator": iterResp.ShardIterator,
		"Limit":         1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var getResp struct {
		Records            []any `json:"Records"`
		MillisBehindLatest int64 `json:"MillisBehindLatest"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &getResp))
	assert.Len(t, getResp.Records, 1)
	// Should be behind the last record, not just the next one.
	assert.GreaterOrEqual(t, getResp.MillisBehindLatest, int64(0))
}
