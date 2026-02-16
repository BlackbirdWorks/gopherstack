package models_test

import (
	"testing"

	"Gopherstack/dynamodb/models"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToSDKPutItemInput(t *testing.T) {
	t.Parallel()

	input := &models.PutItemInput{
		TableName: "test-table",
		Item: map[string]any{
			"pk": map[string]any{"S": "v1"},
		},
		ConditionExpression: "attribute_not_exists(pk)",
	}

	got, err := models.ToSDKPutItemInput(input)
	require.NoError(t, err)

	assert.Equal(t, "test-table", *got.TableName)
	assert.Equal(t, "attribute_not_exists(pk)", *got.ConditionExpression)
	assert.NotNil(t, got.Item["pk"])
}

func TestToSDKGetItemInput(t *testing.T) {
	t.Parallel()

	input := &models.GetItemInput{
		TableName: "test-table",
		Key: map[string]any{
			"pk": map[string]any{"S": "v1"},
		},
	}

	got, err := models.ToSDKGetItemInput(input)
	require.NoError(t, err)

	assert.Equal(t, "test-table", *got.TableName)
	assert.NotNil(t, got.Key["pk"])
}

func TestFromSDKPutItemOutput(t *testing.T) {
	t.Parallel()

	sdkOut := &dynamodb.PutItemOutput{
		Attributes: map[string]types.AttributeValue{
			"old": &types.AttributeValueMemberS{Value: "v0"},
		},
		ConsumedCapacity: &types.ConsumedCapacity{TableName: aws.String("t1")},
		ItemCollectionMetrics: &types.ItemCollectionMetrics{
			ItemCollectionKey:   map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "v"}},
			SizeEstimateRangeGB: []float64{1.0},
		},
	}

	got := models.FromSDKPutItemOutput(sdkOut)
	require.NotNil(t, got.Attributes)
	assert.Equal(t, map[string]any{"S": "v0"}, got.Attributes["old"])
	assert.NotNil(t, got.ConsumedCapacity)
	assert.NotNil(t, got.ItemCollectionMetrics)
}

func TestSDKItemRoundTrip(t *testing.T) {
	t.Parallel()

	item := map[string]any{
		"pk":   map[string]any{"S": "user1"},
		"age":  map[string]any{"N": "25"},
		"data": map[string]any{"B": "YmluYXJ5"},
	}

	sdkItem, err := models.ToSDKItem(item)
	require.NoError(t, err)

	back := models.FromSDKItem(sdkItem)

	if diff := cmp.Diff(item, back); diff != "" {
		t.Errorf("RoundTrip mismatch (-want +got):\n%s", diff)
	}
}

func TestBatchOperations(t *testing.T) {
	t.Parallel()

	t.Run("BatchWrite", func(t *testing.T) {
		t.Parallel()
		input := &models.BatchWriteItemInput{
			RequestItems: map[string][]models.WriteRequest{
				"table1": {
					{
						PutRequest: &models.PutRequest{
							Item: map[string]any{"pk": map[string]any{"S": "v1"}},
						},
					},
					{
						DeleteRequest: &models.DeleteRequest{
							Key: map[string]any{"pk": map[string]any{"S": "v2"}},
						},
					},
				},
			},
		}

		got, err := models.ToSDKBatchWriteItemInput(input)
		require.NoError(t, err)
		assert.Len(t, got.RequestItems["table1"], 2)
	})

	t.Run("BatchGet", func(t *testing.T) {
		t.Parallel()
		input := &models.BatchGetItemInput{
			RequestItems: map[string]models.KeysAndAttributes{
				"table1": {
					Keys: []map[string]any{{"pk": map[string]any{"S": "v1"}}},
				},
			},
		}

		got, err := models.ToSDKBatchGetItemInput(input)
		require.NoError(t, err)
		assert.Len(t, got.RequestItems["table1"].Keys, 1)
	})
}

func TestFromSDKBatchGetItemOutput(t *testing.T) {
	t.Parallel()

	sdkOut := &dynamodb.BatchGetItemOutput{
		Responses: map[string][]map[string]types.AttributeValue{
			"table1": {{"pk": &types.AttributeValueMemberS{Value: "v1"}}},
		},
		UnprocessedKeys: map[string]types.KeysAndAttributes{
			"table2": {
				Keys: []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "v2"}}},
			},
		},
	}

	got := models.FromSDKBatchGetItemOutput(sdkOut)
	assert.Len(t, got.Responses["table1"], 1)
	assert.Len(t, got.UnprocessedKeys["table2"].Keys, 1)
}

func TestQueryScan(t *testing.T) {
	t.Parallel()

	t.Run("Query", func(t *testing.T) {
		t.Parallel()
		input := &models.QueryInput{
			TableName:         "table",
			Limit:             10,
			ExclusiveStartKey: map[string]any{"pk": map[string]any{"S": "k1"}},
		}
		got, err := models.ToSDKQueryInput(input)
		require.NoError(t, err)
		assert.Equal(t, int32(10), *got.Limit)

		sdkOut := &dynamodb.QueryOutput{
			Items:            []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "v1"}}},
			LastEvaluatedKey: map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: "v1"}},
			ConsumedCapacity: &types.ConsumedCapacity{TableName: aws.String("t1")},
		}
		gotOut := models.FromSDKQueryOutput(sdkOut)
		assert.Len(t, gotOut.Items, 1)
		assert.NotNil(t, gotOut.LastEvaluatedKey)
	})

	t.Run("Scan", func(t *testing.T) {
		t.Parallel()
		input := &models.ScanInput{TableName: "table"}
		got, err := models.ToSDKScanInput(input)
		require.NoError(t, err)
		assert.Equal(t, "table", *got.TableName)

		sdkOut := &dynamodb.ScanOutput{
			Items: []map[string]types.AttributeValue{{"pk": &types.AttributeValueMemberS{Value: "v1"}}},
		}
		gotOut := models.FromSDKScanOutput(sdkOut)
		assert.Len(t, gotOut.Items, 1)
	})
}

func TestTransactOperations(t *testing.T) {
	t.Parallel()

	t.Run("TransactWrite", func(t *testing.T) {
		t.Parallel()
		input := &models.TransactWriteItemsInput{
			TransactItems: []models.TransactWriteItem{
				{Put: &models.PutItemInput{TableName: "t1", Item: map[string]any{"pk": map[string]any{"S": "v1"}}}},
				{
					Delete: &models.DeleteItemInput{
						TableName: "t1",
						Key:       map[string]any{"pk": map[string]any{"S": "v2"}},
					},
				},
				{
					Update: &models.UpdateItemInput{
						TableName:        "t1",
						Key:              map[string]any{"pk": map[string]any{"S": "v3"}},
						UpdateExpression: "SET a = :v",
					},
				},
				{
					ConditionCheck: &models.ConditionCheckInput{
						TableName:           "t1",
						Key:                 map[string]any{"pk": map[string]any{"S": "v4"}},
						ConditionExpression: "attribute_exists(pk)",
						ExpressionAttributeValues: map[string]any{
							":v": map[string]any{"S": "val"},
						},
					},
				},
			},
		}

		got, err := models.ToSDKTransactWriteItemsInput(input)
		require.NoError(t, err)
		assert.Len(t, got.TransactItems, 4)
	})

	t.Run("TransactGet", func(t *testing.T) {
		t.Parallel()
		input := &models.TransactGetItemsInput{
			TransactItems: []models.TransactGetItem{
				{Get: &models.GetItemInput{TableName: "t1", Key: map[string]any{"pk": map[string]any{"S": "v1"}}}},
			},
		}

		got, err := models.ToSDKTransactGetItemsInput(input)
		require.NoError(t, err)
		assert.Len(t, got.TransactItems, 1)

		sdkOut := &dynamodb.TransactGetItemsOutput{
			Responses: []types.ItemResponse{
				{
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "v1"},
					},
				},
			},
		}
		gotOut := models.FromSDKTransactGetItemsOutput(sdkOut)
		assert.Len(t, gotOut.Responses, 1)
	})
}

func TestFromSDKBatchWriteItemOutputFull(t *testing.T) {
	t.Parallel()

	sdkOut := &dynamodb.BatchWriteItemOutput{
		UnprocessedItems: map[string][]types.WriteRequest{
			"table1": {
				{
					PutRequest: &types.PutRequest{
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "v1"},
						},
					},
				},
				{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "v2"},
						},
					},
				},
			},
		},
		ConsumedCapacity: []types.ConsumedCapacity{{TableName: aws.String("t1")}},
	}

	got := models.FromSDKBatchWriteItemOutput(sdkOut)
	assert.Len(t, got.UnprocessedItems["table1"], 2)
	assert.Len(t, got.ConsumedCapacity, 1)

	sdkOut.ItemCollectionMetrics = map[string][]types.ItemCollectionMetrics{
		"table1": {
			{
				ItemCollectionKey: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: "v"},
				},
			},
		},
	}
	gotMetrics := models.FromSDKBatchWriteItemOutput(sdkOut)
	assert.Len(t, gotMetrics.ItemCollectionMetrics["table1"], 1)
}

func TestToSDKUpdateTimeToLiveInput(t *testing.T) {
	t.Parallel()

	input := &models.UpdateTimeToLiveInput{
		TableName: "t1",
		TimeToLiveSpecification: models.TimeToLiveSpecification{
			AttributeName: "ttl",
			Enabled:       true,
		},
	}
	got := models.ToSDKUpdateTimeToLiveInput(input)
	assert.Equal(t, "t1", *got.TableName)
	assert.Equal(t, "ttl", *got.TimeToLiveSpecification.AttributeName)
	assert.True(t, *got.TimeToLiveSpecification.Enabled)
}

func TestFromSDKTransactWriteItemsOutput(t *testing.T) {
	t.Parallel()

	sdkOut := &dynamodb.TransactWriteItemsOutput{
		ItemCollectionMetrics: map[string][]types.ItemCollectionMetrics{
			"table1": {
				{
					ItemCollectionKey: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "v"},
					},
				},
			},
		},
		ConsumedCapacity: []types.ConsumedCapacity{{TableName: aws.String("t1")}},
	}
	got := models.FromSDKTransactWriteItemsOutput(sdkOut)
	assert.Len(t, got.ItemCollectionMetrics["table1"], 1)
	assert.Len(t, got.ConsumedCapacity, 1)
}
