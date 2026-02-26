package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Kinesis_StreamLifecycle tests create, list, describe, and delete.
func TestIntegration_Kinesis_StreamLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-stream-" + uuid.NewString()

	// CreateStream
	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// ListStreams
	listOut, err := client.ListStreams(ctx, &kinesis.ListStreamsInput{})
	require.NoError(t, err)
	assert.Contains(t, listOut.StreamNames, streamName)

	// DescribeStream
	descOut, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	assert.Equal(t, streamName, aws.ToString(descOut.StreamDescription.StreamName))
	assert.Equal(t, kinesistypes.StreamStatusActive, descOut.StreamDescription.StreamStatus)
	assert.Len(t, descOut.StreamDescription.Shards, 1)

	// DescribeStreamSummary
	summaryOut, err := client.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	assert.Equal(t, streamName, aws.ToString(summaryOut.StreamDescriptionSummary.StreamName))
	assert.Equal(t, int32(1), aws.ToInt32(summaryOut.StreamDescriptionSummary.OpenShardCount))

	// DeleteStream
	_, err = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)

	// Verify gone
	listOut2, err := client.ListStreams(ctx, &kinesis.ListStreamsInput{})
	require.NoError(t, err)
	assert.NotContains(t, listOut2.StreamNames, streamName)
}

// TestIntegration_Kinesis_PutAndGetRecords tests the full put/get records flow.
func TestIntegration_Kinesis_PutAndGetRecords(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-records-" + uuid.NewString()

	// CreateStream
	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Get shard ID from DescribeStream
	descOut, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, descOut.StreamDescription.Shards)
	shardID := aws.ToString(descOut.StreamDescription.Shards[0].ShardId)

	// PutRecord
	putOut, err := client.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("pk-1"),
		Data:         []byte("hello from integration test"),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(putOut.ShardId))
	assert.NotEmpty(t, aws.ToString(putOut.SequenceNumber))
	firstSeq := aws.ToString(putOut.SequenceNumber)

	// PutRecords (batch)
	putRecordsOut, err := client.PutRecords(ctx, &kinesis.PutRecordsInput{
		StreamName: aws.String(streamName),
		Records: []kinesistypes.PutRecordsRequestEntry{
			{PartitionKey: aws.String("pk-2"), Data: []byte("record 2")},
			{PartitionKey: aws.String("pk-3"), Data: []byte("record 3")},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(0), aws.ToInt32(putRecordsOut.FailedRecordCount))
	assert.Len(t, putRecordsOut.Records, 2)

	// GetShardIterator - TRIM_HORIZON
	iterOut, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(iterOut.ShardIterator))

	// GetRecords
	getOut, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         aws.Int32(10),
	})
	require.NoError(t, err)
	assert.Len(t, getOut.Records, 3) // 1 + 2 batch
	assert.Equal(t, "hello from integration test", string(getOut.Records[0].Data))
	assert.NotEmpty(t, aws.ToString(getOut.NextShardIterator))

	// GetShardIterator - AT_SEQUENCE_NUMBER
	atIterOut, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:             aws.String(streamName),
		ShardId:                aws.String(shardID),
		ShardIteratorType:      kinesistypes.ShardIteratorTypeAtSequenceNumber,
		StartingSequenceNumber: aws.String(firstSeq),
	})
	require.NoError(t, err)

	atGetOut, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: atIterOut.ShardIterator,
		Limit:         aws.Int32(10),
	})
	require.NoError(t, err)
	require.NotEmpty(t, atGetOut.Records)
	assert.Equal(t, firstSeq, aws.ToString(atGetOut.Records[0].SequenceNumber))

	// GetShardIterator - AFTER_SEQUENCE_NUMBER
	afterIterOut, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:             aws.String(streamName),
		ShardId:                aws.String(shardID),
		ShardIteratorType:      kinesistypes.ShardIteratorTypeAfterSequenceNumber,
		StartingSequenceNumber: aws.String(firstSeq),
	})
	require.NoError(t, err)

	afterGetOut, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: afterIterOut.ShardIterator,
		Limit:         aws.Int32(10),
	})
	require.NoError(t, err)
	// AFTER_SEQUENCE_NUMBER skips the first record
	assert.Len(t, afterGetOut.Records, 2)

	// GetShardIterator - LATEST (no new records)
	latestIterOut, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeLatest,
	})
	require.NoError(t, err)

	latestGetOut, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: latestIterOut.ShardIterator,
	})
	require.NoError(t, err)
	assert.Empty(t, latestGetOut.Records)

	// Cleanup
	_, err = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
}

// TestIntegration_Kinesis_ListShards tests the ListShards operation.
func TestIntegration_Kinesis_ListShards(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-shards-" + uuid.NewString()

	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(3),
	})
	require.NoError(t, err)

	listShardsOut, err := client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	assert.Len(t, listShardsOut.Shards, 3)

	for _, shard := range listShardsOut.Shards {
		assert.NotEmpty(t, aws.ToString(shard.ShardId))
	}

	_, err = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
}

// TestIntegration_Kinesis_DataIntegrity verifies data round-trips correctly.
func TestIntegration_Kinesis_DataIntegrity(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-integrity-" + uuid.NewString()

	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	descOut, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	shardID := aws.ToString(descOut.StreamDescription.Shards[0].ShardId)

	// Put several records with unique data
	testData := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	for _, d := range testData {
		_, err = client.PutRecord(ctx, &kinesis.PutRecordInput{
			StreamName:   aws.String(streamName),
			PartitionKey: aws.String("pk"),
			Data:         []byte(d),
		})
		require.NoError(t, err)
	}

	// Read all back from TRIM_HORIZON
	iterOut, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	getOut, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         aws.Int32(100),
	})
	require.NoError(t, err)
	require.Len(t, getOut.Records, len(testData))

	for i, rec := range getOut.Records {
		assert.Equal(t, testData[i], string(rec.Data))
		assert.Equal(t, "pk", aws.ToString(rec.PartitionKey))
		assert.NotEmpty(t, aws.ToString(rec.SequenceNumber))
	}

	_, err = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
}
