package dynamodbstreams_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ddbsdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	ddbbackend "github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodbstreams"
)

// TestHandler_FullLifecycle_StreamsFlow drives the full enable -> put -> iterate ->
// read flow across all four operations (ListStreams, DescribeStream,
// GetShardIterator, GetRecords).
func TestHandler_FullLifecycle_StreamsFlow(t *testing.T) {
	t.Parallel()

	db := ddbbackend.NewInMemoryDB()
	ctx := t.Context()

	const tableName = "LifecycleTable"
	_, err := db.CreateTable(ctx, &ddbsdk.CreateTableInput{
		TableName: aws.String(tableName),
		KeySchema: []ddbtypes.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbtypes.KeyTypeHash},
		},
		AttributeDefinitions: []ddbtypes.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbtypes.ScalarAttributeTypeS},
		},
		BillingMode: ddbtypes.BillingModePayPerRequest,
		StreamSpecification: &ddbtypes.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: ddbtypes.StreamViewTypeNewAndOldImages,
		},
	})
	require.NoError(t, err)

	// Write 3 items: INSERT, MODIFY, REMOVE.
	_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "lifecycle-1"},
		},
	})
	require.NoError(t, err)

	_, err = db.PutItem(ctx, &ddbsdk.PutItemInput{
		TableName: aws.String(tableName),
		Item: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "lifecycle-1"},
		},
	})
	require.NoError(t, err)

	_, err = db.DeleteItem(ctx, &ddbsdk.DeleteItemInput{
		TableName: aws.String(tableName),
		Key: map[string]ddbtypes.AttributeValue{
			"pk": &ddbtypes.AttributeValueMemberS{Value: "lifecycle-1"},
		},
	})
	require.NoError(t, err)

	table, ok := db.GetTable(tableName)
	require.True(t, ok)

	handler := dynamodbstreams.NewHandler(db)

	// Step 1: ListStreams.
	listResp := doRequest(t, handler, "ListStreams",
		fmt.Sprintf(`{"TableName":"%s"}`, tableName))
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	streams, ok := listOut["Streams"].([]any)
	require.True(t, ok)
	require.Len(t, streams, 1)

	// Step 2: DescribeStream.
	descResp := doRequest(t, handler, "DescribeStream",
		fmt.Sprintf(`{"StreamArn":"%s"}`, table.StreamARN))
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	streamDesc := descOut["StreamDescription"].(map[string]any)
	shards := streamDesc["Shards"].([]any)
	require.NotEmpty(t, shards)

	// Step 3: GetShardIterator.
	shardID := shards[0].(map[string]any)["ShardId"].(string)
	iterResp := doRequest(t, handler, "GetShardIterator",
		fmt.Sprintf(`{"StreamArn":"%s","ShardId":"%s","ShardIteratorType":"TRIM_HORIZON"}`,
			table.StreamARN, shardID))
	require.Equal(t, http.StatusOK, iterResp.Code)
	var iterOut map[string]any
	require.NoError(t, json.Unmarshal(iterResp.Body.Bytes(), &iterOut))
	shardIter, ok := iterOut["ShardIterator"].(string)
	require.True(t, ok)

	// Step 4: GetRecords.
	recResp := doRequest(t, handler, "GetRecords",
		fmt.Sprintf(`{"ShardIterator":"%s"}`, shardIter))
	require.Equal(t, http.StatusOK, recResp.Code)
	var recOut map[string]any
	require.NoError(t, json.Unmarshal(recResp.Body.Bytes(), &recOut))
	records, ok := recOut["Records"].([]any)
	require.True(t, ok)
	require.Len(t, records, 3, "must return all 3 stream events")

	// Verify event names.
	assert.Equal(t, "INSERT", records[0].(map[string]any)["eventName"])
	assert.Equal(t, "MODIFY", records[1].(map[string]any)["eventName"])
	assert.Equal(t, "REMOVE", records[2].(map[string]any)["eventName"])

	// Step 5: Use NextShardIterator to confirm no more records.
	nextIter := recOut["NextShardIterator"].(string)
	emptyResp := doRequest(t, handler, "GetRecords",
		fmt.Sprintf(`{"ShardIterator":"%s"}`, nextIter))
	require.Equal(t, http.StatusOK, emptyResp.Code)
	var emptyOut map[string]any
	require.NoError(t, json.Unmarshal(emptyResp.Body.Bytes(), &emptyOut))
	emptyRecords, ok := emptyOut["Records"].([]any)
	if ok {
		assert.Empty(t, emptyRecords, "NextShardIterator must return no new records")
	}
}
