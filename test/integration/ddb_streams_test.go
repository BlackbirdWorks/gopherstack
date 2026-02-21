package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams"
	"github.com/aws/aws-sdk-go-v2/service/dynamodbstreams/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_Streams_FullFlow(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	ddbClient := createDynamoDBClient(t)
	streamsClient := createDynamoDBStreamsClient(t)

	tableName := "StreamsTestTable-" + uuid.NewString()
	ctx := t.Context()

	// 1. Create a table with Streams enabled
	_, err := ddbClient.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
	})
	require.NoError(t, err)

	// Clean up table after test
	defer func() {
		_, _ = ddbClient.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	}()

	// 2. Perform operations: PutItem (INSERT), UpdateItem (MODIFY), DeleteItem (REMOVE)
	// INSERT
	_, err = ddbClient.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]ddbtypes.AttributeValue{
			"pk":   &ddbtypes.AttributeValueMemberS{Value: "item-1"},
			"data": &ddbtypes.AttributeValueMemberS{Value: "initial"},
		},
	})
	require.NoError(t, err)

	// MODIFY
	_, err = ddbClient.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableName),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "item-1"},
		},
		UpdateExpression: aws.String("SET #d = :val"),
		ExpressionAttributeNames: map[string]string{
			"#d": "data",
		},
		ExpressionAttributeValues: map[string]ddbtypes.AttributeValue{
			":val": &ddbtypes.AttributeValueMemberS{Value: "updated"},
		},
	})
	require.NoError(t, err)

	// REMOVE
	_, err = ddbClient.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "item-1"},
		},
	})
	require.NoError(t, err)

	// 3. ListStreams
	listStreams, err := streamsClient.ListStreams(ctx, &dynamodbstreams.ListStreamsInput{
		TableName: aws.String(tableName),
	})
	require.NoError(t, err)
	require.Len(t, listStreams.Streams, 1)
	streamARN := listStreams.Streams[0].StreamArn
	require.NotNil(t, streamARN)

	// 4. DescribeStream to get Shard ID
	describeStream, err := streamsClient.DescribeStream(ctx, &dynamodbstreams.DescribeStreamInput{
		StreamArn: streamARN,
	})
	require.NoError(t, err)
	require.NotEmpty(t, describeStream.StreamDescription.Shards)
	shardID := describeStream.StreamDescription.Shards[0].ShardId

	// 5. GetShardIterator
	shardIterator, err := streamsClient.GetShardIterator(ctx, &dynamodbstreams.GetShardIteratorInput{
		StreamArn:         streamARN,
		ShardId:           shardID,
		ShardIteratorType: types.ShardIteratorTypeTrimHorizon,
	})
	require.NoError(t, err)
	require.NotNil(t, shardIterator.ShardIterator)

	// 6. GetRecords
	getRecords, err := streamsClient.GetRecords(ctx, &dynamodbstreams.GetRecordsInput{
		ShardIterator: shardIterator.ShardIterator,
	})
	require.NoError(t, err)

	// We expect 3 records
	require.Len(t, getRecords.Records, 3)

	// Verify INSERT
	assert.Equal(t, types.OperationTypeInsert, getRecords.Records[0].EventName)
	assert.NotNil(t, getRecords.Records[0].Dynamodb.NewImage)
	assert.Nil(t, getRecords.Records[0].Dynamodb.OldImage)
	assert.Equal(t, "item-1", getRecords.Records[0].Dynamodb.NewImage["pk"].(*types.AttributeValueMemberS).Value)

	// Verify MODIFY
	assert.Equal(t, types.OperationTypeModify, getRecords.Records[1].EventName)
	assert.NotNil(t, getRecords.Records[1].Dynamodb.NewImage)
	assert.NotNil(t, getRecords.Records[1].Dynamodb.OldImage)
	assert.Equal(t, "updated", getRecords.Records[1].Dynamodb.NewImage["data"].(*types.AttributeValueMemberS).Value)
	assert.Equal(t, "initial", getRecords.Records[1].Dynamodb.OldImage["data"].(*types.AttributeValueMemberS).Value)

	// Verify REMOVE
	assert.Equal(t, types.OperationTypeRemove, getRecords.Records[2].EventName)
	assert.Nil(t, getRecords.Records[2].Dynamodb.NewImage)
	assert.NotNil(t, getRecords.Records[2].Dynamodb.OldImage)
	assert.Equal(t, "item-1", getRecords.Records[2].Dynamodb.OldImage["pk"].(*types.AttributeValueMemberS).Value)
}
