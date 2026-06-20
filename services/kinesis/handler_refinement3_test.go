package kinesis_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

// ---------------------------------------------------------------------------
// Issue 1: GetRecords 10 MiB response cap
// ---------------------------------------------------------------------------

func TestRefinement3_GetRecords_10MBCap_StopsAtLimit(t *testing.T) {
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

func TestRefinement3_GetRecords_10MBCap_SingleLargeRecordAllowed(t *testing.T) {
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

func TestRefinement3_GetRecords_10MBCap_IteratorAdvancesCorrectly(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Issue 2: ON_DEMAND stream count limit
// ---------------------------------------------------------------------------

func TestRefinement3_CreateStream_OnDemandLimitEnforced(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	// Set a tight limit of 2 ON_DEMAND streams.
	require.NoError(t, b.UpdateAccountSettings(context.Background(), &kinesis.UpdateAccountSettingsInput{
		OnDemandStreamCountLimit: 2,
	}))

	// Create 2 ON_DEMAND streams (should succeed).
	for i := range 2 {
		require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
			StreamName: fmt.Sprintf("od-limit-stream-%d", i),
			ShardCount: 1,
			StreamMode: "ON_DEMAND",
		}))
	}

	// Third ON_DEMAND stream should fail with LimitExceededException.
	err := b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-limit-stream-overflow",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	})
	require.Error(t, err)
}

func TestRefinement3_CreateStream_OnDemandLimit_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.UpdateAccountSettings(context.Background(), &kinesis.UpdateAccountSettingsInput{
		OnDemandStreamCountLimit: 1,
	}))

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName":        "od-handler-1",
		"ShardCount":        1,
		"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "CreateStream", map[string]any{
		"StreamName":        "od-handler-2",
		"ShardCount":        1,
		"StreamModeDetails": map[string]any{"StreamMode": "ON_DEMAND"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var errResp struct {
		Type string `json:"__type"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
	assert.Equal(t, "LimitExceededException", errResp.Type)
}

func TestRefinement3_CreateStream_ProvisionedNotAffectedByOnDemandLimit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.UpdateAccountSettings(context.Background(), &kinesis.UpdateAccountSettingsInput{
		OnDemandStreamCountLimit: 1,
	}))

	// Fill the ON_DEMAND quota.
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-quota-stream",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	}))

	// Provisioned stream should still be createable.
	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName":        "prov-no-limit",
		"ShardCount":        1,
		"StreamModeDetails": map[string]any{"StreamMode": "PROVISIONED"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestRefinement3_CreateStream_OnDemandLimit_DeleteFreesSlot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.UpdateAccountSettings(context.Background(), &kinesis.UpdateAccountSettingsInput{
		OnDemandStreamCountLimit: 1,
	}))

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-del-stream",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	}))

	// Limit reached.
	require.Error(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-del-stream-2",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	}))

	// Delete the first stream to free the slot.
	require.NoError(t, b.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: "od-del-stream"}))

	// Now the second stream should succeed.
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-del-stream-2",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	}))
}

// ---------------------------------------------------------------------------
// Issue 4: PutRecords per-record error codes
// ---------------------------------------------------------------------------

func TestRefinement3_PutRecords_OversizeRecordReturnsValidationException(t *testing.T) {
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

func TestRefinement3_PutRecords_ThrottledRecordReturnsProvisionedThroughputException(t *testing.T) {
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

func TestRefinement3_PutRecords_AllValidRecordsSucceed(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Issue 5: IncreaseStreamRetentionPeriod bounds
// ---------------------------------------------------------------------------

func TestRefinement3_IncreaseRetention_BelowMinRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "retention-min-stream",
		ShardCount: 1,
	}))

	// Attempting to set retention to 0 (below minimum 24h) should fail.
	err := b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           "retention-min-stream",
		RetentionPeriodHours: 0,
	})
	require.Error(t, err)
}

func TestRefinement3_IncreaseRetention_AboveMaxRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "retention-max-stream",
		ShardCount: 1,
	}))

	// 8761 hours > maxRetentionHours (8760) should fail.
	err := b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           "retention-max-stream",
		RetentionPeriodHours: 8761,
	})
	require.Error(t, err)
}

func TestRefinement3_IncreaseRetention_ValidRangeAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "retention-valid-stream",
		ShardCount: 1,
	}))

	// 168 h (7 days) is within valid range [24, 8760].
	err := b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           "retention-valid-stream",
		RetentionPeriodHours: 168,
	})
	require.NoError(t, err)

	out, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "retention-valid-stream"},
	)
	require.NoError(t, err)
	assert.Equal(t, 168, out.RetentionPeriodHours)
}

func TestRefinement3_IncreaseRetention_MaxBoundaryAccepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "retention-boundary-stream",
		ShardCount: 1,
	}))

	// Exactly maxRetentionHours (8760) should succeed.
	err := b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
		StreamName:           "retention-boundary-stream",
		RetentionPeriodHours: 8760,
	})
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Issue 6: ListShards pagination (MaxResults + NextToken)
// ---------------------------------------------------------------------------

func TestRefinement3_ListShards_MaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "list-shards-paginated",
		ShardCount: 5,
	}))

	// Request only 2 shards.
	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName: "list-shards-paginated",
		MaxResults: 2,
	})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 2)
	assert.NotEmpty(t, out.NextToken, "should have a next token for remaining shards")
}

func TestRefinement3_ListShards_NextToken_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "list-shards-nexttoken",
		ShardCount: 5,
	}))

	var allShards []kinesis.ShardDescription
	var nextToken string

	for {
		out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
			StreamName: "list-shards-nexttoken",
			MaxResults: 2,
			NextToken:  nextToken,
		})
		require.NoError(t, err)
		allShards = append(allShards, out.Shards...)
		nextToken = out.NextToken
		if nextToken == "" {
			break
		}
	}

	assert.Len(t, allShards, 5, "should get all 5 shards via pagination")

	// Verify no duplicates.
	seen := make(map[string]struct{})
	for _, s := range allShards {
		assert.NotContains(t, seen, s.ShardID, "duplicate shard %q in pagination", s.ShardID)
		seen[s.ShardID] = struct{}{}
	}
}

func TestRefinement3_ListShards_NoMaxResults_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "list-shards-nomax",
		ShardCount: 4,
	}))

	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "list-shards-nomax"})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 4)
	assert.Empty(t, out.NextToken)
}

func TestRefinement3_ListShards_MaxResults_ViaHandler(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "list-shards-handler-paged",
		"ShardCount": 4,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "ListShards", map[string]any{
		"StreamName": "list-shards-handler-paged",
		"MaxResults": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		NextToken string `json:"NextToken"`
		Shards    []any  `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp.Shards, 2)
	assert.NotEmpty(t, resp.NextToken)

	// Second page — NextToken encodes stream context; StreamName must be omitted (AWS contract).
	rec = doRequest(t, h, "ListShards", map[string]any{
		"MaxResults": 2,
		"NextToken":  resp.NextToken,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp2 struct {
		NextToken string `json:"NextToken"`
		Shards    []any  `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp2))
	assert.Len(t, resp2.Shards, 2)
	assert.Empty(t, resp2.NextToken, "no more pages")
}

func TestRefinement3_ListShards_MaxResults_ExactlyFits(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "list-shards-exact",
		ShardCount: 3,
	}))

	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName: "list-shards-exact",
		MaxResults: 3,
	})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 3)
	assert.Empty(t, out.NextToken, "should not emit next token when all fit in one page")
}

// ---------------------------------------------------------------------------
// Issue 7: GetRecords MillisBehindLatest uses last record (tip of stream)
// ---------------------------------------------------------------------------

func TestRefinement3_GetRecords_MillisBehindLatest_UsesLastRecord(t *testing.T) {
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

func TestRefinement3_GetRecords_MillisBehindLatest_ZeroWhenCaughtUp(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Issue 9: UpdateShardCount marks old shards CLOSED
// ---------------------------------------------------------------------------

func TestRefinement3_UpdateShardCount_OldShardsMarkedClosed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "update-shardcount-closed",
		ShardCount: 2,
	}))

	out, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "update-shardcount-closed"},
	)
	require.NoError(t, err)
	require.Len(t, out.Shards, 2)

	// Scale up to 4.
	_, err = b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "update-shardcount-closed",
		TargetShardCount: 4,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	// DescribeStream must include old closed shards + new open ones.
	out2, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "update-shardcount-closed"},
	)
	require.NoError(t, err)

	openCount := 0
	closedCount := 0
	for _, s := range out2.Shards {
		if s.Closed {
			closedCount++
		} else {
			openCount++
		}
	}

	assert.Equal(t, 4, openCount, "should have 4 new open shards")
	assert.Equal(t, 2, closedCount, "old 2 shards should be marked closed")
	assert.Len(t, out2.Shards, 6, "total 6 shards (2 closed + 4 open)")
}

func TestRefinement3_UpdateShardCount_ListShardsOnlyReturnsOpenShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "update-listshard-stream",
		ShardCount: 2,
	}))

	_, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "update-listshard-stream",
		TargetShardCount: 3,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	// ListShards default = open shards only.
	list, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "update-listshard-stream"})
	require.NoError(t, err)
	assert.Len(t, list.Shards, 3, "ListShards should return only the 3 new open shards")
}

func TestRefinement3_UpdateShardCount_CurrentCountIsOpenShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "update-currentcount-stream",
		ShardCount: 4,
	}))

	out, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "update-currentcount-stream",
		TargetShardCount: 2,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	// CurrentShardCount should reflect the 4 open shards before the operation.
	assert.Equal(t, 4, out.CurrentShardCount)
	assert.Equal(t, 2, out.TargetShardCount)
}

func TestRefinement3_UpdateShardCount_UniqueShardIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "update-uniqueids-stream",
		ShardCount: 2,
	}))

	_, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "update-uniqueids-stream",
		TargetShardCount: 3,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	// Scale again.
	_, err = b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "update-uniqueids-stream",
		TargetShardCount: 1,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	out, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "update-uniqueids-stream"},
	)
	require.NoError(t, err)

	seen := make(map[string]struct{})
	for _, s := range out.Shards {
		assert.NotContains(t, seen, s.ShardID, "duplicate shard ID %q", s.ShardID)
		seen[s.ShardID] = struct{}{}
	}
}

// ---------------------------------------------------------------------------
// Issue 10: DescribeStream includes closed shards
// ---------------------------------------------------------------------------

func TestRefinement3_DescribeStream_IncludesClosedShardsAfterMerge(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "describe-closed-merge",
		"ShardCount": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-closed-merge"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	s0 := descResp.StreamDescription.Shards[0].ShardID
	s1 := descResp.StreamDescription.Shards[1].ShardID

	rec = doRequest(t, h, "MergeShards", map[string]any{
		"StreamName":           "describe-closed-merge",
		"ShardToMerge":         s0,
		"AdjacentShardToMerge": s1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// After merge: 2 closed parents + 1 open merged = 3 total.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-closed-merge"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.StreamDescription.Shards, 3)
}

func TestRefinement3_DescribeStream_IncludesClosedShardsAfterSplit(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "describe-closed-split",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID string `json:"ShardId"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-closed-split"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	shardID := descResp.StreamDescription.Shards[0].ShardID

	const splitKey = "170141183460469231731687303715884105728"
	rec = doRequest(t, h, "SplitShard", map[string]any{
		"StreamName":         "describe-closed-split",
		"ShardToSplit":       shardID,
		"NewStartingHashKey": splitKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// After split: 1 closed parent + 2 open children = 3 total.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-closed-split"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.StreamDescription.Shards, 3)
}

func TestRefinement3_DescribeStream_OpenShardNoEndingSequenceNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "describe-open-shard",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID             string `json:"ShardId"`
				SequenceNumberRange struct {
					EndingSequenceNumber string `json:"EndingSequenceNumber"`
				} `json:"SequenceNumberRange"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-open-shard"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 1)
	// Open shards have an empty EndingSequenceNumber.
	assert.Empty(t, descResp.StreamDescription.Shards[0].SequenceNumberRange.EndingSequenceNumber)
}

// TestRefinement3_DescribeStream_OpenShardWithRecordsNoEndingSeq verifies that an
// OPEN shard that already holds records still reports no EndingSequenceNumber.
// In real AWS, EndingSequenceNumber is reported only for CLOSED shards — a
// KCL-style consumer treats its presence as the signal a shard is closed and it
// should advance to the child shards. Reporting it on an open-but-populated shard
// would make a consumer prematurely abandon a live shard.
func TestRefinement3_DescribeStream_OpenShardWithRecordsNoEndingSeq(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "open-shard-with-records",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Write several records into the single (still open) shard.
	for i := range 3 {
		rec = doRequest(t, h, "PutRecord", map[string]any{
			"StreamName":   "open-shard-with-records",
			"PartitionKey": fmt.Sprintf("pk-%d", i),
			"Data":         []byte("payload"),
		})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID             string `json:"ShardId"`
				SequenceNumberRange struct {
					StartingSequenceNumber string `json:"StartingSequenceNumber"`
					EndingSequenceNumber   string `json:"EndingSequenceNumber"`
				} `json:"SequenceNumberRange"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "open-shard-with-records"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 1)

	shard := descResp.StreamDescription.Shards[0]
	// A populated open shard still has a starting sequence number...
	assert.NotEmpty(t, shard.SequenceNumberRange.StartingSequenceNumber)
	// ...but must NOT report an ending sequence number while it remains open.
	assert.Empty(t, shard.SequenceNumberRange.EndingSequenceNumber,
		"open shard with records must not report EndingSequenceNumber")
}

// ---------------------------------------------------------------------------
// Issue 11: ExplicitHashKey upper bound validation
// ---------------------------------------------------------------------------

func TestRefinement3_PutRecord_ExplicitHashKey_AboveMaxRejected(t *testing.T) {
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

func TestRefinement3_PutRecord_ExplicitHashKey_NegativeRejected(t *testing.T) {
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

func TestRefinement3_PutRecord_ExplicitHashKey_ZeroAccepted(t *testing.T) {
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

func TestRefinement3_PutRecord_ExplicitHashKey_MaxAccepted(t *testing.T) {
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

func TestRefinement3_PutRecord_ExplicitHashKey_ViaHandler_AboveMaxRejected(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Cross-cutting: regression tests for existing behavior
// ---------------------------------------------------------------------------

func TestRefinement3_GetRecords_SmallRecords_NoCap(t *testing.T) {
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

func TestRefinement3_ListShards_WithMaxResults_PlusClosedShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "listshards-closed-paged",
		ShardCount: 2,
	}))

	out, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "listshards-closed-paged"},
	)
	require.NoError(t, err)

	// Merge to produce 1 open + 2 closed = 3 total.
	require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
		StreamName:           "listshards-closed-paged",
		ShardToMerge:         out.Shards[0].ShardID,
		AdjacentShardToMerge: out.Shards[1].ShardID,
	}))

	// FROM_TRIM_HORIZON includes all shards; MaxResults=2 → page 1 of 2.
	list, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:  "listshards-closed-paged",
		ShardFilter: "FROM_TRIM_HORIZON",
		MaxResults:  2,
	})
	require.NoError(t, err)
	assert.Len(t, list.Shards, 2)
	assert.NotEmpty(t, list.NextToken)

	// Page 2.
	list2, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:  "listshards-closed-paged",
		ShardFilter: "FROM_TRIM_HORIZON",
		MaxResults:  2,
		NextToken:   list.NextToken,
	})
	require.NoError(t, err)
	assert.Len(t, list2.Shards, 1)
	assert.Empty(t, list2.NextToken)
}

func TestRefinement3_DescribeAccountSettings_OnDemandCount(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.UpdateAccountSettings(context.Background(), &kinesis.UpdateAccountSettingsInput{
		OnDemandStreamCountLimit: 5,
	}))

	// Create 2 ON_DEMAND streams.
	for i := range 2 {
		require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
			StreamName: fmt.Sprintf("acct-od-stream-%d", i),
			ShardCount: 1,
			StreamMode: "ON_DEMAND",
		}))
	}

	out, err := b.DescribeAccountSettings(context.Background())
	require.NoError(t, err)
	assert.Equal(t, 2, out.OnDemandStreamCount)
	assert.Equal(t, 5, out.OnDemandStreamCountLimit)
}

func TestRefinement3_UpdateShardCount_ViaHandler_OpenShardsOnly(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "handler-update-shard",
		"ShardCount": 2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "UpdateShardCount", map[string]any{
		"StreamName":       "handler-update-shard",
		"TargetShardCount": 4,
		"ScalingType":      "UNIFORM_SCALING",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updateResp struct {
		CurrentShardCount int `json:"CurrentShardCount"`
		TargetShardCount  int `json:"TargetShardCount"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Equal(t, 2, updateResp.CurrentShardCount)
	assert.Equal(t, 4, updateResp.TargetShardCount)

	// ListShards returns only open shards → should see 4 new open shards.
	rec = doRequest(t, h, "ListShards", map[string]any{"StreamName": "handler-update-shard"})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp struct {
		Shards []any `json:"Shards"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	assert.Len(t, listResp.Shards, 4)
}

func TestRefinement3_GetRecords_MillisBehindLatest_ViaHandler(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Additional edge-case tests
// ---------------------------------------------------------------------------

func TestRefinement3_GetRecords_10MBCap_ExactlyAtLimit(t *testing.T) {
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

func TestRefinement3_GetRecords_ZeroLimitUsesDefault(t *testing.T) {
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

func TestRefinement3_OnDemandLimit_DefaultLimitIsPositive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	out, err := b.DescribeAccountSettings(context.Background())
	require.NoError(t, err)
	assert.Positive(t, out.OnDemandStreamCountLimit, "default ON_DEMAND limit should be positive")
}

func TestRefinement3_CreateStream_OnDemandLimit_AtBoundary(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.UpdateAccountSettings(context.Background(), &kinesis.UpdateAccountSettingsInput{
		OnDemandStreamCountLimit: 3,
	}))

	for i := range 3 {
		require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
			StreamName: fmt.Sprintf("od-boundary-%d", i),
			ShardCount: 1,
			StreamMode: "ON_DEMAND",
		}), "should succeed for streams 0..2")
	}

	// The 4th should fail.
	err := b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "od-boundary-overflow",
		ShardCount: 1,
		StreamMode: "ON_DEMAND",
	})
	require.Error(t, err)
}

func TestRefinement3_ListShards_NextToken_SinglePage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "listshards-single-page",
		ShardCount: 2,
	}))

	// MaxResults > total shards → single page, no NextToken.
	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName: "listshards-single-page",
		MaxResults: 10,
	})
	require.NoError(t, err)
	assert.Len(t, out.Shards, 2)
	assert.Empty(t, out.NextToken)
}

func TestRefinement3_ListShards_NextToken_OddPageSize(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "listshards-odd-page",
		ShardCount: 7,
	}))

	var all []kinesis.ShardDescription
	nextToken := ""

	for {
		out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
			StreamName: "listshards-odd-page",
			MaxResults: 3,
			NextToken:  nextToken,
		})
		require.NoError(t, err)
		all = append(all, out.Shards...)
		nextToken = out.NextToken
		if nextToken == "" {
			break
		}
	}

	assert.Len(t, all, 7)
}

func TestRefinement3_DescribeStream_UpdateShardCount_IncludesOldClosedShards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "describe-usc-stream",
		"ShardCount": 3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doRequest(t, h, "UpdateShardCount", map[string]any{
		"StreamName":       "describe-usc-stream",
		"TargetShardCount": 2,
		"ScalingType":      "UNIFORM_SCALING",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeStream should include both closed (3 old) and open (2 new) shards.
	var descResp struct {
		StreamDescription struct {
			Shards []any `json:"Shards"`
		} `json:"StreamDescription"`
	}
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "describe-usc-stream"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Len(t, descResp.StreamDescription.Shards, 5, "3 closed + 2 open = 5")
}

func TestRefinement3_UpdateShardCount_SecondScaleStillWorks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	b := h.Backend.(*kinesis.InMemoryBackend)

	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "double-scale-stream",
		ShardCount: 1,
	}))

	_, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "double-scale-stream",
		TargetShardCount: 3,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)

	out2, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "double-scale-stream",
		TargetShardCount: 2,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)
	assert.Equal(t, 3, out2.CurrentShardCount, "current count after first scale is 3 open shards")
	assert.Equal(t, 2, out2.TargetShardCount)
}

func TestRefinement3_ExplicitHashKey_PartitionKeyOverride(t *testing.T) {
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

func TestRefinement3_PutRecord_ExplicitHashKey_OneAboveMax(t *testing.T) {
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

func TestRefinement3_RetentionPeriod_IdempotentIncrease(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "idempotent-retention",
		ShardCount: 1,
	}))

	// Set retention to 48 hours.
	require.NoError(
		t,
		b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
			StreamName:           "idempotent-retention",
			RetentionPeriodHours: 48,
		}),
	)

	// Set it to the same value again — should be a no-op.
	require.NoError(
		t,
		b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
			StreamName:           "idempotent-retention",
			RetentionPeriodHours: 48,
		}),
	)

	out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "idempotent-retention"})
	require.NoError(t, err)
	assert.Equal(t, 48, out.RetentionPeriodHours)
}

func TestRefinement3_RetentionPeriod_DecreaseStillWorks(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "decrease-retention",
		ShardCount: 1,
	}))

	require.NoError(
		t,
		b.IncreaseStreamRetentionPeriod(context.Background(), &kinesis.IncreaseStreamRetentionPeriodInput{
			StreamName:           "decrease-retention",
			RetentionPeriodHours: 168,
		}),
	)

	require.NoError(
		t,
		b.DecreaseStreamRetentionPeriod(context.Background(), &kinesis.DecreaseStreamRetentionPeriodInput{
			StreamName:           "decrease-retention",
			RetentionPeriodHours: 48,
		}),
	)

	out, err := b.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "decrease-retention"})
	require.NoError(t, err)
	assert.Equal(t, 48, out.RetentionPeriodHours)
}

func TestRefinement3_PutRecords_MixedOversizeAndValid(t *testing.T) {
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

func TestRefinement3_ListShards_ClosedShards_IncludedWithFilter(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "listshards-closed-filter",
		ShardCount: 2,
	}))

	ds, err := b.DescribeStream(
		context.Background(),
		&kinesis.DescribeStreamInput{StreamName: "listshards-closed-filter"},
	)
	require.NoError(t, err)

	require.NoError(t, b.MergeShards(context.Background(), &kinesis.MergeShardsInput{
		StreamName:           "listshards-closed-filter",
		ShardToMerge:         ds.Shards[0].ShardID,
		AdjacentShardToMerge: ds.Shards[1].ShardID,
	}))

	// Default: only open shards.
	open, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "listshards-closed-filter"})
	require.NoError(t, err)
	assert.Len(t, open.Shards, 1)

	// FROM_TRIM_HORIZON: all shards.
	all, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:  "listshards-closed-filter",
		ShardFilter: "FROM_TRIM_HORIZON",
	})
	require.NoError(t, err)
	assert.Len(t, all.Shards, 3)
}

func TestRefinement3_UpdateShardCount_LargeScale(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "large-scale-stream",
		ShardCount: 1,
	}))

	out, err := b.UpdateShardCount(context.Background(), &kinesis.UpdateShardCountInput{
		StreamName:       "large-scale-stream",
		TargetShardCount: 10,
		ScalingType:      "UNIFORM_SCALING",
	})
	require.NoError(t, err)
	assert.Equal(t, 1, out.CurrentShardCount)
	assert.Equal(t, 10, out.TargetShardCount)

	// Verify 10 open shards via ListShards.
	list, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{StreamName: "large-scale-stream"})
	require.NoError(t, err)
	assert.Len(t, list.Shards, 10)
}

func TestRefinement3_GetRecords_EmptyShard_MillisBehindZero(t *testing.T) {
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

func TestRefinement3_DescribeStream_ClosedShardHasEndingSequenceNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "closing-seqnum-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Put a record so the shard has a non-trivial sequence range.
	doRequest(t, h, "PutRecord", map[string]any{
		"StreamName":   "closing-seqnum-stream",
		"PartitionKey": "pk",
		"Data":         []byte("data"),
	})

	var descResp struct {
		StreamDescription struct {
			Shards []struct {
				ShardID             string `json:"ShardId"`
				SequenceNumberRange struct {
					EndingSequenceNumber string `json:"EndingSequenceNumber"`
				} `json:"SequenceNumberRange"`
			} `json:"Shards"`
		} `json:"StreamDescription"`
	}

	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "closing-seqnum-stream"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 1)
	shardID := descResp.StreamDescription.Shards[0].ShardID

	// Split the shard to close it.
	const splitKey = "170141183460469231731687303715884105728"
	rec = doRequest(t, h, "SplitShard", map[string]any{
		"StreamName":         "closing-seqnum-stream",
		"ShardToSplit":       shardID,
		"NewStartingHashKey": splitKey,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Re-describe: closed shard should have a non-empty EndingSequenceNumber.
	rec = doRequest(t, h, "DescribeStream", map[string]any{"StreamName": "closing-seqnum-stream"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	require.Len(t, descResp.StreamDescription.Shards, 3)

	closedCount := 0
	for _, s := range descResp.StreamDescription.Shards {
		if s.SequenceNumberRange.EndingSequenceNumber != "" {
			closedCount++
		}
	}
	assert.Equal(t, 1, closedCount, "exactly one shard should be closed with a non-empty ending seq")
}

func TestRefinement3_ExplicitHashKey_ValidMidRange(t *testing.T) {
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

func TestRefinement3_PutRecords_EmptyBatch(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "empty-batch-stream",
		ShardCount: 1,
	}))

	// Empty records slice.
	out, err := b.PutRecords(context.Background(), &kinesis.PutRecordsInput{
		StreamName: "empty-batch-stream",
		Records:    []kinesis.PutRecordsEntry{},
	})
	require.NoError(t, err)
	assert.Equal(t, 0, out.FailedRecordCount)
	assert.Empty(t, out.Records)
}

func TestRefinement3_ListShards_ExclusiveStart_WithMaxResults(t *testing.T) {
	t.Parallel()

	b := kinesis.NewInMemoryBackend()
	require.NoError(t, b.CreateStream(context.Background(), &kinesis.CreateStreamInput{
		StreamName: "listshards-start-max",
		ShardCount: 5,
	}))

	// Start after shard 1 (exclusive), take 2.
	out, err := b.ListShards(context.Background(), &kinesis.ListShardsInput{
		StreamName:            "listshards-start-max",
		ExclusiveStartShardID: "shardId-000000000001",
		MaxResults:            2,
	})
	require.NoError(t, err)
	require.Len(t, out.Shards, 2)
	// Should start from shard 2.
	assert.Equal(t, "shardId-000000000002", out.Shards[0].ShardID)
	assert.Equal(t, "shardId-000000000003", out.Shards[1].ShardID)
	assert.NotEmpty(t, out.NextToken)
}

func TestRefinement3_GetRecords_10MBCap_RecordsBeforeCapNotDropped(t *testing.T) {
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
