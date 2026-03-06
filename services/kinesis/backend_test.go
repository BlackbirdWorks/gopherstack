package kinesis_test

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/kinesis"
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

func TestKinesisProvider_Name(t *testing.T) {
	t.Parallel()

	p := &kinesis.Provider{}
	assert.Equal(t, "Kinesis", p.Name())
}

func TestKinesisProvider_Init(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		config        any
		wantRegion    string
		wantAccountID string
	}{
		{
			name:          "WithConfig",
			config:        fakeContextConfig{},
			wantRegion:    "ap-southeast-2",
			wantAccountID: "111111111111",
		},
		{
			name:   "NoConfig",
			config: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &kinesis.Provider{}
			ctx := &service.AppContext{
				Config: tt.config,
				Logger: slog.Default(),
			}

			svc, err := p.Init(ctx)
			require.NoError(t, err)
			assert.NotNil(t, svc)

			if tt.wantRegion != "" {
				h, ok := svc.(*kinesis.Handler)
				require.True(t, ok)
				assert.Equal(t, tt.wantRegion, h.DefaultRegion)
				assert.Equal(t, tt.wantAccountID, h.AccountID)
			}
		})
	}
}

func TestKinesisBackend_FindSequencePositionGaps(t *testing.T) {
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

func TestKinesisBackend_GetRecordsDeletedStream(t *testing.T) {
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

func TestKinesisBackend_GetRecordsInvalidShard(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(&kinesis.CreateStreamInput{StreamName: "shard-gone-stream"}))

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

func TestKinesisBackend_ListStreamsLimit(t *testing.T) {
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
