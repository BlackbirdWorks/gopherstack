package integration_test

import (
	"math/big"
	"sort"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const hashKeyBase = 10

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

// TestIntegration_Kinesis_EnhancedFanOut tests the full enhanced fan-out consumer lifecycle.
func TestIntegration_Kinesis_EnhancedFanOut(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-efo-" + uuid.NewString()
	consumerName := "my-consumer-" + uuid.NewString()

	// CreateStream
	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Get stream ARN
	descOut, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	streamARN := aws.ToString(descOut.StreamDescription.StreamARN)
	require.NotEmpty(t, streamARN)
	shardID := aws.ToString(descOut.StreamDescription.Shards[0].ShardId)

	// RegisterStreamConsumer
	regOut, err := client.RegisterStreamConsumer(ctx, &kinesis.RegisterStreamConsumerInput{
		StreamARN:    aws.String(streamARN),
		ConsumerName: aws.String(consumerName),
	})
	require.NoError(t, err)
	assert.Equal(t, consumerName, aws.ToString(regOut.Consumer.ConsumerName))
	assert.Equal(t, kinesistypes.ConsumerStatusActive, regOut.Consumer.ConsumerStatus)
	assert.NotEmpty(t, aws.ToString(regOut.Consumer.ConsumerARN))
	consumerARN := aws.ToString(regOut.Consumer.ConsumerARN)

	// DescribeStreamConsumer by ARN
	descConsumerOut, err := client.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
		ConsumerARN: aws.String(consumerARN),
	})
	require.NoError(t, err)
	assert.Equal(t, consumerName, aws.ToString(descConsumerOut.ConsumerDescription.ConsumerName))
	assert.Equal(t, streamARN, aws.ToString(descConsumerOut.ConsumerDescription.StreamARN))

	// DescribeStreamConsumer by stream + name
	descConsumerOut2, err := client.DescribeStreamConsumer(ctx, &kinesis.DescribeStreamConsumerInput{
		StreamARN:    aws.String(streamARN),
		ConsumerName: aws.String(consumerName),
	})
	require.NoError(t, err)
	assert.Equal(t, consumerName, aws.ToString(descConsumerOut2.ConsumerDescription.ConsumerName))

	// ListStreamConsumers
	listConsumersOut, err := client.ListStreamConsumers(ctx, &kinesis.ListStreamConsumersInput{
		StreamARN: aws.String(streamARN),
	})
	require.NoError(t, err)
	require.Len(t, listConsumersOut.Consumers, 1)
	assert.Equal(t, consumerName, aws.ToString(listConsumersOut.Consumers[0].ConsumerName))

	// Put a record
	_, err = client.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("pk-1"),
		Data:         []byte("efo-test-payload"),
	})
	require.NoError(t, err)

	// SubscribeToShard
	subOut, err := client.SubscribeToShard(ctx, &kinesis.SubscribeToShardInput{
		ConsumerARN: aws.String(consumerARN),
		ShardId:     aws.String(shardID),
		StartingPosition: &kinesistypes.StartingPosition{
			Type: kinesistypes.ShardIteratorTypeTrimHorizon,
		},
	})
	require.NoError(t, err)

	stream := subOut.GetStream()

	var got []string
	for event := range stream.Events() {
		if ev, ok := event.(*kinesistypes.SubscribeToShardEventStreamMemberSubscribeToShardEvent); ok {
			for _, r := range ev.Value.Records {
				got = append(got, string(r.Data))
			}
		}
	}
	require.NoError(t, stream.Err())
	assert.Contains(t, got, "efo-test-payload")

	// DeregisterStreamConsumer
	_, err = client.DeregisterStreamConsumer(ctx, &kinesis.DeregisterStreamConsumerInput{
		ConsumerARN: aws.String(consumerARN),
	})
	require.NoError(t, err)

	// Verify consumer is gone
	listConsumersOut2, err := client.ListStreamConsumers(ctx, &kinesis.ListStreamConsumersInput{
		StreamARN: aws.String(streamARN),
	})
	require.NoError(t, err)
	assert.Empty(t, listConsumersOut2.Consumers)

	// Cleanup
	_, err = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
}

// TestIntegration_Kinesis_UpdateShardCount tests resharding a stream.
func TestIntegration_Kinesis_UpdateShardCount(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-reshard-" + uuid.NewString()

	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	reshardOut, err := client.UpdateShardCount(ctx, &kinesis.UpdateShardCountInput{
		StreamName:       aws.String(streamName),
		TargetShardCount: aws.Int32(2),
		ScalingType:      kinesistypes.ScalingTypeUniformScaling,
	})
	require.NoError(t, err)
	assert.Equal(t, int32(1), aws.ToInt32(reshardOut.CurrentShardCount))
	assert.Equal(t, int32(2), aws.ToInt32(reshardOut.TargetShardCount))

	// Verify new shard count
	listShardsOut, err := client.ListShards(ctx, &kinesis.ListShardsInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	assert.Len(t, listShardsOut.Shards, 2)

	_, err = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
}

// TestIntegration_Kinesis_EnhancedMonitoring tests enabling and disabling enhanced monitoring.
func TestIntegration_Kinesis_EnhancedMonitoring(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-monitor-" + uuid.NewString()

	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Enable monitoring
	enableOut, err := client.EnableEnhancedMonitoring(ctx, &kinesis.EnableEnhancedMonitoringInput{
		StreamName: aws.String(streamName),
		ShardLevelMetrics: []kinesistypes.MetricsName{
			kinesistypes.MetricsNameIncomingBytes,
			kinesistypes.MetricsNameOutgoingRecords,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, streamName, aws.ToString(enableOut.StreamName))
	assert.Empty(t, enableOut.CurrentShardLevelMetrics)
	assert.Len(t, enableOut.DesiredShardLevelMetrics, 2)

	// Disable one metric
	disableOut, err := client.DisableEnhancedMonitoring(ctx, &kinesis.DisableEnhancedMonitoringInput{
		StreamName:        aws.String(streamName),
		ShardLevelMetrics: []kinesistypes.MetricsName{kinesistypes.MetricsNameIncomingBytes},
	})
	require.NoError(t, err)
	assert.Len(t, disableOut.CurrentShardLevelMetrics, 2)
	assert.Len(t, disableOut.DesiredShardLevelMetrics, 1)

	_, err = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
}

// TestIntegration_Kinesis_GetShardIteratorAtTimestamp tests the AT_TIMESTAMP iterator type.
func TestIntegration_Kinesis_GetShardIteratorAtTimestamp(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-ts-iterator-" + uuid.NewString()

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

	// Put a record
	_, err = client.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("pk"),
		Data:         []byte("ts-test"),
	})
	require.NoError(t, err)

	// AT_TIMESTAMP at epoch should return the record
	epochTime := aws.Time(time.Unix(0, 0))
	iterOut, err := client.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeAtTimestamp,
		Timestamp:         epochTime,
	})
	require.NoError(t, err)

	getOut, err := client.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
		Limit:         aws.Int32(10),
	})
	require.NoError(t, err)
	assert.Len(t, getOut.Records, 1)
	assert.Equal(t, "ts-test", string(getOut.Records[0].Data))

	_, err = client.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
}

// TestIntegration_Kinesis_SplitShard_RoundTrip round-trips a shard split
// through SplitShard and ListShards, verifying the two child shards carry the
// real parent linkage (parentShardId) and correctly partition the parent's
// hash key range — a resharding op no prior real-client test has exercised.
func TestIntegration_Kinesis_SplitShard_RoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-split-" + uuid.NewString()

	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStream(cleanupCtx, &kinesis.DeleteStreamInput{StreamName: aws.String(streamName)})
	})

	descOut, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)
	require.Len(t, descOut.StreamDescription.Shards, 1)

	parent := descOut.StreamDescription.Shards[0]
	parentID := aws.ToString(parent.ShardId)

	start, ok := new(big.Int).SetString(aws.ToString(parent.HashKeyRange.StartingHashKey), hashKeyBase)
	require.True(t, ok)
	end, ok := new(big.Int).SetString(aws.ToString(parent.HashKeyRange.EndingHashKey), hashKeyBase)
	require.True(t, ok)
	mid := new(big.Int).Add(start, new(big.Int).Div(new(big.Int).Sub(end, start), big.NewInt(2)))

	_, err = client.SplitShard(ctx, &kinesis.SplitShardInput{
		StreamName:         aws.String(streamName),
		ShardToSplit:       aws.String(parentID),
		NewStartingHashKey: aws.String(mid.String()),
	})
	require.NoError(t, err)

	listOut, err := client.ListShards(ctx, &kinesis.ListShardsInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)
	require.Len(
		t, listOut.Shards, 2,
		"the parent shard should be closed and excluded from the default open-shard listing",
	)

	children := append([]kinesistypes.Shard{}, listOut.Shards...)
	sort.Slice(children, func(i, j int) bool {
		si, _ := new(big.Int).SetString(aws.ToString(children[i].HashKeyRange.StartingHashKey), hashKeyBase)
		sj, _ := new(big.Int).SetString(aws.ToString(children[j].HashKeyRange.StartingHashKey), hashKeyBase)

		return si.Cmp(sj) < 0
	})

	for _, s := range children {
		assert.Equal(t, parentID, aws.ToString(s.ParentShardId))
	}

	assert.Equal(t, start.String(), aws.ToString(children[0].HashKeyRange.StartingHashKey))
	assert.Equal(t, mid.String(), aws.ToString(children[1].HashKeyRange.StartingHashKey))
	assert.Equal(t, end.String(), aws.ToString(children[1].HashKeyRange.EndingHashKey))

	oneLess := new(big.Int).Sub(mid, big.NewInt(1))
	assert.Equal(t, oneLess.String(), aws.ToString(children[0].HashKeyRange.EndingHashKey))
}

// TestIntegration_Kinesis_MergeShards_RoundTrip round-trips a shard merge
// through MergeShards and ListShards, verifying the resulting shard carries
// both real parent references (parentShardId, adjacentParentShardId) and the
// union of the two parents' hash key ranges.
func TestIntegration_Kinesis_MergeShards_RoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-merge-" + uuid.NewString()

	_, err := client.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(2),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteStream(cleanupCtx, &kinesis.DeleteStreamInput{StreamName: aws.String(streamName)})
	})

	descOut, err := client.DescribeStream(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)
	require.Len(t, descOut.StreamDescription.Shards, 2)

	shards := append([]kinesistypes.Shard{}, descOut.StreamDescription.Shards...)
	sort.Slice(shards, func(i, j int) bool {
		si, _ := new(big.Int).SetString(aws.ToString(shards[i].HashKeyRange.StartingHashKey), hashKeyBase)
		sj, _ := new(big.Int).SetString(aws.ToString(shards[j].HashKeyRange.StartingHashKey), hashKeyBase)

		return si.Cmp(sj) < 0
	})

	lowShardID := aws.ToString(shards[0].ShardId)
	highShardID := aws.ToString(shards[1].ShardId)
	fullStart := aws.ToString(shards[0].HashKeyRange.StartingHashKey)
	fullEnd := aws.ToString(shards[1].HashKeyRange.EndingHashKey)

	_, err = client.MergeShards(ctx, &kinesis.MergeShardsInput{
		StreamName:           aws.String(streamName),
		ShardToMerge:         aws.String(lowShardID),
		AdjacentShardToMerge: aws.String(highShardID),
	})
	require.NoError(t, err)

	listOut, err := client.ListShards(ctx, &kinesis.ListShardsInput{StreamName: aws.String(streamName)})
	require.NoError(t, err)
	require.Len(t, listOut.Shards, 1, "both parents should be closed, leaving only the merged shard open")

	merged := listOut.Shards[0]
	assert.Equal(t, lowShardID, aws.ToString(merged.ParentShardId))
	assert.Equal(t, highShardID, aws.ToString(merged.AdjacentParentShardId))
	assert.Equal(t, fullStart, aws.ToString(merged.HashKeyRange.StartingHashKey))
	assert.Equal(t, fullEnd, aws.ToString(merged.HashKeyRange.EndingHashKey))
}

// TestIntegration_Kinesis_ResourcePolicy_RoundTrip round-trips a resource
// policy through PutResourcePolicy and GetResourcePolicy, verifying the raw
// policy document text survives unmangled. Never exercised by a real client
// before this test.
func TestIntegration_Kinesis_ResourcePolicy_RoundTrip(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createKinesisClient(t)
	ctx := t.Context()

	streamName := "test-respolicy-" + uuid.NewString()
	resourceARN := "arn:aws:kinesis:us-east-1:000000000000:stream/" + streamName
	policyDoc := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow",` +
		`"Principal":{"AWS":"arn:aws:iam::111111111111:root"},` +
		`"Action":"kinesis:GetRecords","Resource":"` + resourceARN + `"}]}`

	_, err := client.PutResourcePolicy(ctx, &kinesis.PutResourcePolicyInput{
		ResourceARN: aws.String(resourceARN),
		Policy:      aws.String(policyDoc),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteResourcePolicy(cleanupCtx, &kinesis.DeleteResourcePolicyInput{
			ResourceARN: aws.String(resourceARN),
		})
	})

	getOut, err := client.GetResourcePolicy(ctx, &kinesis.GetResourcePolicyInput{
		ResourceARN: aws.String(resourceARN),
	})
	require.NoError(t, err)
	assert.JSONEq(t, policyDoc, aws.ToString(getOut.Policy))
}
