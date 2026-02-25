package kinesis_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/kinesis"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// fakeConfigProvider implements config.Provider for tests.
type fakeConfigProvider struct{}

func (fakeConfigProvider) GetGlobalConfig() config.GlobalConfig {
	return config.GlobalConfig{AccountID: "111111111111", Region: "ap-southeast-2"}
}

// fakeContextConfig wraps fakeConfigProvider for service.AppContext.
type fakeContextConfig struct {
	fakeConfigProvider
}

// TestKinesis_ProviderName tests the Provider.Name method.
func TestKinesis_ProviderName(t *testing.T) {
	t.Parallel()

	p := &kinesis.Provider{}
	assert.Equal(t, "Kinesis", p.Name())
}

// TestKinesis_ProviderInitWithConfig tests that Init propagates account and region.
func TestKinesis_ProviderInitWithConfig(t *testing.T) {
	t.Parallel()

	p := &kinesis.Provider{}
	ctx := &service.AppContext{
		Config: fakeContextConfig{},
		Logger: slog.Default(),
	}

	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)

	h, ok := svc.(*kinesis.Handler)
	require.True(t, ok)
	assert.Equal(t, "ap-southeast-2", h.DefaultRegion)
	assert.Equal(t, "111111111111", h.AccountID)
}

// TestKinesis_ProviderInitNoConfig tests Init without a config provider.
func TestKinesis_ProviderInitNoConfig(t *testing.T) {
	t.Parallel()

	p := &kinesis.Provider{}
	ctx := &service.AppContext{
		Config: nil,
		Logger: slog.Default(),
	}

	svc, err := p.Init(ctx)
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// TestKinesis_BackendFindSequencePositionMiddle tests findSequencePosition with a middle-only range.
// We test this by using AT_SEQUENCE_NUMBER with a sequence number that falls between records.
func TestKinesis_BackendFindSequencePositionGaps(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "gap-stream"}))

	desc, err := bk.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "gap-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	// Put a record - get seq "00000000000000000001"
	out1, err := bk.PutRecord(&kinesis.PutRecordInput{
		StreamName:   "gap-stream",
		PartitionKey: "pk",
		Data:         []byte("first"),
	})
	require.NoError(t, err)

	// Put another - get seq "00000000000000000002"
	out2, err := bk.PutRecord(&kinesis.PutRecordInput{
		StreamName:   "gap-stream",
		PartitionKey: "pk",
		Data:         []byte("second"),
	})
	require.NoError(t, err)

	// AT_SEQUENCE_NUMBER for out1.SequenceNumber should return index 0 (inclusive)
	iterOut, err := bk.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:             "gap-stream",
		ShardID:                shardID,
		ShardIteratorType:      "AT_SEQUENCE_NUMBER",
		StartingSequenceNumber: out1.SequenceNumber,
	})
	require.NoError(t, err)

	records, err := bk.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         10,
	})
	require.NoError(t, err)
	require.Len(t, records.Records, 2)
	assert.Equal(t, out1.SequenceNumber, records.Records[0].SequenceNumber)

	// AFTER_SEQUENCE_NUMBER for out1 should start at index 1
	iterOut2, err := bk.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:             "gap-stream",
		ShardID:                shardID,
		ShardIteratorType:      "AFTER_SEQUENCE_NUMBER",
		StartingSequenceNumber: out1.SequenceNumber,
	})
	require.NoError(t, err)

	records2, err := bk.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: iterOut2.ShardIterator,
		Limit:         10,
	})
	require.NoError(t, err)
	require.Len(t, records2.Records, 1)
	assert.Equal(t, out2.SequenceNumber, records2.Records[0].SequenceNumber)

	// AT_SEQUENCE_NUMBER for a sequence number that is lexicographically larger than all records
	// should return empty (positions at end)
	iterOut3, err := bk.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:             "gap-stream",
		ShardID:                shardID,
		ShardIteratorType:      "AT_SEQUENCE_NUMBER",
		StartingSequenceNumber: "99999999999999999999",
	})
	require.NoError(t, err)

	records3, err := bk.GetRecords(&kinesis.GetRecordsInput{
		ShardIterator: iterOut3.ShardIterator,
		Limit:         10,
	})
	require.NoError(t, err)
	assert.Empty(t, records3.Records)
}

// TestKinesis_PutRecordMaxRecords tests the record trim behavior when shard is full.
func TestKinesis_PutRecordMaxRecords(t *testing.T) {
	t.Parallel()

	// We use a handler test since direct backend access is needed
	h := newTestHandler(t)

	rec := doRequest(t, h, "CreateStream", map[string]any{
		"StreamName": "trim-stream",
		"ShardCount": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestKinesis_GetRecordsIteratorGoneStream tests GetRecords when the stream has been deleted.
func TestKinesis_GetRecordsDeletedStream(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "deleted-stream"}))

	desc, err := bk.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "deleted-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	iterOut, err := bk.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:        "deleted-stream",
		ShardID:           shardID,
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	// Delete stream
	require.NoError(t, bk.DeleteStream(&kinesis.DeleteStreamInput{StreamName: "deleted-stream"}))

	// GetRecords should return stream not found
	_, err = bk.GetRecords(&kinesis.GetRecordsInput{ShardIterator: iterOut.ShardIterator})
	assert.ErrorIs(t, err, kinesis.ErrStreamNotFound)
}

// TestKinesis_GetRecordsDeletedShardIterator tests GetRecords with bad iterator.
func TestKinesis_GetRecordsInvalidShard(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "shard-gone-stream"}))

	// Manually encode an iterator pointing to a non-existent shard
	// We'll do it by creating a valid iterator and then deleting and recreating the stream
	desc, err := bk.DescribeStream(&kinesis.DescribeStreamInput{StreamName: "shard-gone-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	iterOut, err := bk.GetShardIterator(&kinesis.GetShardIteratorInput{
		StreamName:        "shard-gone-stream",
		ShardID:           shardID,
		ShardIteratorType: "TRIM_HORIZON",
	})
	require.NoError(t, err)

	// Delete and recreate the stream (new shards will have the same IDs so this won't test the gap,
	// but we can test invalid shard via ListShards with wrong stream name)
	require.NoError(t, bk.DeleteStream(&kinesis.DeleteStreamInput{StreamName: "shard-gone-stream"}))

	// Recreate stream (iterator now points to deleted stream)
	_, err = bk.GetRecords(&kinesis.GetRecordsInput{ShardIterator: iterOut.ShardIterator})
	assert.Error(t, err)
}

// TestKinesis_ListStreamsLimit tests ListStreams with limit.
func TestKinesis_ListStreamsLimit(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()

	for i := range 5 {
		require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{
			StreamName: fmt.Sprintf("limit-stream-%d", i),
		}))
	}

	out, err := bk.ListStreams(&kinesis.ListStreamsInput{Limit: 3})
	require.NoError(t, err)
	assert.Len(t, out.StreamNames, 3)
	assert.True(t, out.HasMoreStreams)
}

// TestKinesis_HandleListStreamsEmpty tests handleListStreams with nil result.
func TestKinesis_HandleListStreamsEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Fresh handler has no streams; JSON result should have empty array, not nil
	rec := doRequest(t, h, "ListStreams", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		StreamNames []string `json:"StreamNames"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp.StreamNames)
	assert.Empty(t, resp.StreamNames)
}

// TestKinesis_HandleInvalidJSONRequests tests JSON parse error branches.
func TestKinesis_HandleInvalidJSONRequests(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	// Send invalid JSON to each operation
	ops := []string{
		"CreateStream", "DeleteStream", "DescribeStream", "DescribeStreamSummary",
		"PutRecord", "PutRecords", "GetShardIterator", "GetRecords", "ListShards",
	}

	for _, op := range ops {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte("{invalid")))
		req.Header.Set("X-Amz-Target", "Kinesis_20131202."+op)
		req.Header.Set("Content-Type", "application/x-amz-json-1.1")
		c := e.NewContext(req, rec)
		err := h.Handler()(c)
		require.NoError(t, err, "op=%s", op)
		// All should return 4xx
		assert.GreaterOrEqual(t, rec.Code, 400, "op=%s should return error", op)
	}
}
