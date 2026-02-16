//go: integration

package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_Coverage(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)
	ctx := t.Context()
	tableName := "CoverageTest-" + uuid.NewString()

	// 1. Create Table with TTL and secondary index for more coverage
	_, err := client.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(tableName),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(5),
			WriteCapacityUnits: aws.Int64(5),
		},
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})

	// 2. Test NULL, BOOL, and BINARY types
	t.Run("AttributeTypes", func(t *testing.T) {
		item := map[string]types.AttributeValue{
			"pk":   &types.AttributeValueMemberS{Value: "types-item"},
			"null": &types.AttributeValueMemberNULL{Value: true},
			"bool": &types.AttributeValueMemberBOOL{Value: true},
			"bin":  &types.AttributeValueMemberB{Value: []byte("binary data")},
			"ss":   &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
			"ns":   &types.AttributeValueMemberNS{Value: []string{"1", "2"}},
			"bs":   &types.AttributeValueMemberBS{Value: [][]byte{[]byte("b1"), []byte("b2")}},
		}

		_, pErr := client.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item:      item,
		})
		require.NoError(t, pErr)

		getOut, err := client.GetItem(ctx, &dynamodb.GetItemInput{
			TableName: aws.String(tableName),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "types-item"},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, getOut.Item)

		assert.IsType(t, &types.AttributeValueMemberNULL{}, getOut.Item["null"])
		assert.IsType(t, &types.AttributeValueMemberBOOL{}, getOut.Item["bool"])
		assert.IsType(t, &types.AttributeValueMemberB{}, getOut.Item["bin"])
		assert.IsType(t, &types.AttributeValueMemberSS{}, getOut.Item["ss"])
		assert.IsType(t, &types.AttributeValueMemberNS{}, getOut.Item["ns"])
		assert.IsType(t, &types.AttributeValueMemberBS{}, getOut.Item["bs"])
	})

	// 3. Test TTL Operations
	t.Run("TTL", func(t *testing.T) {
		_, uErr := client.UpdateTimeToLive(ctx, &dynamodb.UpdateTimeToLiveInput{
			TableName: aws.String(tableName),
			TimeToLiveSpecification: &types.TimeToLiveSpecification{
				AttributeName: aws.String("ttl_attr"),
				Enabled:       aws.Bool(true),
			},
		})
		require.NoError(t, uErr)

		descOut, err := client.DescribeTimeToLive(ctx, &dynamodb.DescribeTimeToLiveInput{
			TableName: aws.String(tableName),
		})
		require.NoError(t, err)
		assert.Equal(t, types.TimeToLiveStatusEnabled, descOut.TimeToLiveDescription.TimeToLiveStatus)
		assert.Equal(t, "ttl_attr", *descOut.TimeToLiveDescription.AttributeName)
	})

	// 4. Test Transactions
	t.Run("Transactions", func(t *testing.T) {
		_, tErr := client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
			TransactItems: []types.TransactWriteItem{
				{
					Put: &types.Put{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-item-1"},
							"v":  &types.AttributeValueMemberS{Value: "val1"},
						},
					},
				},
				{
					Update: &types.Update{
						TableName: aws.String(tableName),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-item-2"},
						},
						UpdateExpression: aws.String("SET v = :v"),
						ExpressionAttributeValues: map[string]types.AttributeValue{
							":v": &types.AttributeValueMemberS{Value: "val2"},
						},
					},
				},
			},
		})
		require.NoError(t, tErr)

		getOut, err := client.TransactGetItems(ctx, &dynamodb.TransactGetItemsInput{
			TransactItems: []types.TransactGetItem{
				{
					Get: &types.Get{
						TableName: aws.String(tableName),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-item-1"},
						},
					},
				},
				{
					Get: &types.Get{
						TableName: aws.String(tableName),
						Key: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: "tx-item-2"},
						},
					},
				},
			},
		})
		require.NoError(t, err)
		require.Len(t, getOut.Responses, 2)
		assert.Equal(t, "val1", getOut.Responses[0].Item["v"].(*types.AttributeValueMemberS).Value)
		assert.Equal(t, "val2", getOut.Responses[1].Item["v"].(*types.AttributeValueMemberS).Value)
	})
}
