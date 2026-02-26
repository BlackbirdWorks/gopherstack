package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdaesdktypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_Kinesis_EventSourceMapping tests the ESM CRUD lifecycle.
func TestIntegration_Kinesis_EventSourceMapping(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	kinesisClient := createKinesisClient(t)
	lambdaClient := createLambdaClient(t)
	ctx := t.Context()

	streamName := "esm-test-stream-" + uuid.NewString()

	// Create stream
	_, err := kinesisClient.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Get stream ARN
	descOut, err := kinesisClient.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	streamARN := aws.ToString(descOut.StreamDescriptionSummary.StreamARN)
	require.NotEmpty(t, streamARN)

	// Create ESM
	createESMOut, err := lambdaClient.CreateEventSourceMapping(ctx, &lambdasdk.CreateEventSourceMappingInput{
		EventSourceArn:   aws.String(streamARN),
		FunctionName:     aws.String("nonexistent-function"),
		StartingPosition: lambdaesdktypes.EventSourcePositionTrimHorizon,
		BatchSize:        aws.Int32(25),
	})
	require.NoError(t, err)
	assert.NotEmpty(t, aws.ToString(createESMOut.UUID))
	assert.Equal(t, int32(25), aws.ToInt32(createESMOut.BatchSize))
	esmUUID := aws.ToString(createESMOut.UUID)

	// GetEventSourceMapping
	getESMOut, err := lambdaClient.GetEventSourceMapping(ctx, &lambdasdk.GetEventSourceMappingInput{
		UUID: aws.String(esmUUID),
	})
	require.NoError(t, err)
	assert.Equal(t, esmUUID, aws.ToString(getESMOut.UUID))
	assert.Equal(t, streamARN, aws.ToString(getESMOut.EventSourceArn))

	// ListEventSourceMappings
	listESMOut, err := lambdaClient.ListEventSourceMappings(ctx, &lambdasdk.ListEventSourceMappingsInput{
		FunctionName: aws.String("nonexistent-function"),
	})
	require.NoError(t, err)
	found := false
	for _, m := range listESMOut.EventSourceMappings {
		if aws.ToString(m.UUID) == esmUUID {
			found = true

			break
		}
	}
	assert.True(t, found, "ESM should appear in list")

	// DeleteEventSourceMapping
	deleteESMOut, err := lambdaClient.DeleteEventSourceMapping(ctx, &lambdasdk.DeleteEventSourceMappingInput{
		UUID: aws.String(esmUUID),
	})
	require.NoError(t, err)
	assert.Equal(t, esmUUID, aws.ToString(deleteESMOut.UUID))

	// Verify gone
	_, err = lambdaClient.GetEventSourceMapping(ctx, &lambdasdk.GetEventSourceMappingInput{
		UUID: aws.String(esmUUID),
	})
	require.Error(t, err)

	// Cleanup stream
	_, err = kinesisClient.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
}

// TestIntegration_Kinesis_EventSourceMapping_WithRecords verifies that after ESM creation,
// records can be put and read from the associated Kinesis stream.
func TestIntegration_Kinesis_EventSourceMapping_WithRecords(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	kinesisClient := createKinesisClient(t)
	lambdaClient := createLambdaClient(t)
	ctx := t.Context()

	streamName := "esm-records-" + uuid.NewString()

	// Create stream
	_, err := kinesisClient.CreateStream(ctx, &kinesis.CreateStreamInput{
		StreamName: aws.String(streamName),
		ShardCount: aws.Int32(1),
	})
	require.NoError(t, err)

	// Get stream ARN
	descOut, err := kinesisClient.DescribeStreamSummary(ctx, &kinesis.DescribeStreamSummaryInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	streamARN := aws.ToString(descOut.StreamDescriptionSummary.StreamARN)

	// Create ESM
	createESMOut, err := lambdaClient.CreateEventSourceMapping(ctx, &lambdasdk.CreateEventSourceMappingInput{
		EventSourceArn:   aws.String(streamARN),
		FunctionName:     aws.String("my-processor"),
		StartingPosition: lambdaesdktypes.EventSourcePositionTrimHorizon,
	})
	require.NoError(t, err)
	esmUUID := aws.ToString(createESMOut.UUID)

	// Put records to stream
	_, err = kinesisClient.PutRecord(ctx, &kinesis.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String("pk"),
		Data:         []byte("esm test record"),
	})
	require.NoError(t, err)

	// Verify records can be read
	descStream, err := kinesisClient.DescribeStream(ctx, &kinesis.DescribeStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
	require.NotEmpty(t, descStream.StreamDescription.Shards)

	shardID := aws.ToString(descStream.StreamDescription.Shards[0].ShardId)
	iterOut, err := kinesisClient.GetShardIterator(ctx, &kinesis.GetShardIteratorInput{
		StreamName:        aws.String(streamName),
		ShardId:           aws.String(shardID),
		ShardIteratorType: kinesistypes.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)

	getOut, err := kinesisClient.GetRecords(ctx, &kinesis.GetRecordsInput{
		ShardIterator: iterOut.ShardIterator,
	})
	require.NoError(t, err)
	assert.Len(t, getOut.Records, 1)
	assert.Equal(t, "esm test record", string(getOut.Records[0].Data))

	// Cleanup
	_, err = lambdaClient.DeleteEventSourceMapping(ctx, &lambdasdk.DeleteEventSourceMappingInput{
		UUID: aws.String(esmUUID),
	})
	require.NoError(t, err)

	_, err = kinesisClient.DeleteStream(ctx, &kinesis.DeleteStreamInput{
		StreamName: aws.String(streamName),
	})
	require.NoError(t, err)
}
