package kinesis_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kinesis"
)

func TestKinesisBackend_FindSequencePositionGaps(t *testing.T) {
	t.Parallel()

	bk := kinesis.NewInMemoryBackend()
	require.NoError(t, bk.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: "gap-stream"}))

	desc, err := bk.DescribeStream(context.Background(), &kinesis.DescribeStreamInput{StreamName: "gap-stream"})
	require.NoError(t, err)
	shardID := desc.Shards[0].ShardID

	// Put a record - get seq "00000000000000000001"
	out1, err := bk.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "gap-stream",
		PartitionKey: "pk",
		Data:         []byte("first"),
	})
	require.NoError(t, err)

	// Put another - get seq "00000000000000000002"
	out2, err := bk.PutRecord(context.Background(), &kinesis.PutRecordInput{
		StreamName:   "gap-stream",
		PartitionKey: "pk",
		Data:         []byte("second"),
	})
	require.NoError(t, err)

	// AT_SEQUENCE_NUMBER for out1.SequenceNumber should return index 0 (inclusive)
	iterOut, err := bk.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:             "gap-stream",
		ShardID:                shardID,
		ShardIteratorType:      "AT_SEQUENCE_NUMBER",
		StartingSequenceNumber: out1.SequenceNumber,
	})
	require.NoError(t, err)

	records, err := bk.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         10,
	})
	require.NoError(t, err)
	require.Len(t, records.Records, 2)
	assert.Equal(t, out1.SequenceNumber, records.Records[0].SequenceNumber)

	// AFTER_SEQUENCE_NUMBER for out1 should start at index 1
	iterOut2, err := bk.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:             "gap-stream",
		ShardID:                shardID,
		ShardIteratorType:      "AFTER_SEQUENCE_NUMBER",
		StartingSequenceNumber: out1.SequenceNumber,
	})
	require.NoError(t, err)

	records2, err := bk.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut2.ShardIterator,
		Limit:         10,
	})
	require.NoError(t, err)
	require.Len(t, records2.Records, 1)
	assert.Equal(t, out2.SequenceNumber, records2.Records[0].SequenceNumber)

	// AT_SEQUENCE_NUMBER for a sequence number that is lexicographically larger than all records
	// should return empty (positions at end)
	iterOut3, err := bk.GetShardIterator(context.Background(), &kinesis.GetShardIteratorInput{
		StreamName:             "gap-stream",
		ShardID:                shardID,
		ShardIteratorType:      "AT_SEQUENCE_NUMBER",
		StartingSequenceNumber: "99999999999999999999",
	})
	require.NoError(t, err)

	records3, err := bk.GetRecords(context.Background(), &kinesis.GetRecordsInput{
		ShardIterator: iterOut3.ShardIterator,
		Limit:         10,
	})
	require.NoError(t, err)
	assert.Empty(t, records3.Records)
}
