package dynamodb_test

import (
	"testing"

	"Gopherstack/dynamodb"

	"github.com/aws/aws-sdk-go-v2/aws"
	dynamodb_sdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDynamoDB_ExtraTypes(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	tableName := "ExtraTypesTable"
	createTableHelper(t, db, tableName, "pk")

	t.Run("AllTypes", func(t *testing.T) {
		item := map[string]any{
			"pk":   map[string]any{"S": "item1"},
			"null": map[string]any{"NULL": true},
			"bool": map[string]any{"BOOL": false},
			"bin":  map[string]any{"B": "Ymlu"}, // base64 for "bin"
			"ss":   map[string]any{"SS": []any{"a", "b"}},
			"ns":   map[string]any{"NS": []any{"1", "2"}},
			"bs":   map[string]any{"BS": []any{"YjE="}}, // base64 for "b1"
		}

		sdkInputItem, err := dynamodb.ToSDKItem(item)
		require.NoError(t, err)

		putInput := &dynamodb_sdk.PutItemInput{
			TableName: aws.String(tableName),
			Item:      sdkInputItem,
		}
		_, err = db.PutItem(putInput)
		require.NoError(t, err)

		getInput := &dynamodb_sdk.GetItemInput{
			TableName: &tableName,
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item1"},
			},
		}
		getResp, err := db.GetItem(getInput)
		require.NoError(t, err)
		require.NotNil(t, getResp.Item)

		assert.IsType(t, &types.AttributeValueMemberNULL{}, getResp.Item["null"])
		assert.IsType(t, &types.AttributeValueMemberBOOL{}, getResp.Item["bool"])
		assert.IsType(t, &types.AttributeValueMemberB{}, getResp.Item["bin"])
		assert.IsType(t, &types.AttributeValueMemberSS{}, getResp.Item["ss"])
		assert.IsType(t, &types.AttributeValueMemberNS{}, getResp.Item["ns"])
		assert.IsType(t, &types.AttributeValueMemberBS{}, getResp.Item["bs"])
	})
}

func TestDynamoDB_TTL_Operations(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	tableName := "TTLTable"
	createTableHelper(t, db, tableName, "pk")

	t.Run("UpdateAndDescribe", func(t *testing.T) {
		updateInput := &dynamodb_sdk.UpdateTimeToLiveInput{
			TableName: &tableName,
			TimeToLiveSpecification: &types.TimeToLiveSpecification{
				AttributeName: aws.String("ttl_attr"),
				Enabled:       aws.Bool(true),
			},
		}
		_, err := db.UpdateTimeToLive(updateInput)
		require.NoError(t, err)

		describeInput := &dynamodb_sdk.DescribeTimeToLiveInput{
			TableName: &tableName,
		}
		descResp, err := db.DescribeTimeToLive(describeInput)
		require.NoError(t, err)
		assert.Equal(t, types.TimeToLiveStatusEnabled, descResp.TimeToLiveDescription.TimeToLiveStatus)
		assert.Equal(t, "ttl_attr", *descResp.TimeToLiveDescription.AttributeName)
	})
}

func TestDynamoDB_Transaction_Operations(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()
	tableName := "TxTable"
	createTableHelper(t, db, tableName, "pk")

	t.Run("WriteAndGet", func(t *testing.T) {
		writeInput := &dynamodb_sdk.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName: &tableName,
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx1"},
							"v":  &types.AttributeValueMemberS{Value: "val1"},
						},
					},
				},
				{
					Put: &types.Put{
						TableName: &tableName,
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx2"},
							"v":  &types.AttributeValueMemberS{Value: "val2"},
						},
					},
				},
			},
		}
		_, err := db.TransactWriteItems(writeInput)
		require.NoError(t, err)

		getInput := &dynamodb_sdk.TransactGetItemsInput{
			TransactItems: []types.TransactGetItem{
				{
					Get: &types.Get{
						TableName: &tableName,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx1"},
						},
					},
				},
				{
					Get: &types.Get{
						TableName: &tableName,
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx2"},
						},
					},
				},
			},
		}
		getResp, err := db.TransactGetItems(getInput)
		require.NoError(t, err)
		require.Len(t, getResp.Responses, 2)
		assert.Equal(t, "val1", getResp.Responses[0].Item["v"].(*types.AttributeValueMemberS).Value)
		assert.Equal(t, "val2", getResp.Responses[1].Item["v"].(*types.AttributeValueMemberS).Value)
	})
}
