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
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)
	ctx := t.Context()
	tableName := "CoverageTest-" + uuid.NewString()

	_, createErr := client.CreateTable(ctx, &dynamodb.CreateTableInput{
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
	require.NoError(t, createErr)

	t.Cleanup(func() {
		client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{
			TableName: aws.String(tableName),
		})
	})

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "AttributeTypes_NullBoolBinaryAndSets",
			run: func(t *testing.T) {
				t.Helper()
				item := map[string]types.AttributeValue{
					"pk":   &types.AttributeValueMemberS{Value: "types-item"},
					"null": &types.AttributeValueMemberNULL{Value: true},
					"bool": &types.AttributeValueMemberBOOL{Value: true},
					"bin":  &types.AttributeValueMemberB{Value: []byte("binary data")},
					"ss":   &types.AttributeValueMemberSS{Value: []string{"a", "b"}},
					"ns":   &types.AttributeValueMemberNS{Value: []string{"1", "2"}},
					"bs":   &types.AttributeValueMemberBS{Value: [][]byte{[]byte("b1"), []byte("b2")}},
				}

				_, pErr := client.PutItem(t.Context(), &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item:      item,
				})
				require.NoError(t, pErr)

				getOut, err := client.GetItem(t.Context(), &dynamodb.GetItemInput{
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
			},
		},
		{
			name: "TTL_EnableAndDescribe",
			run: func(t *testing.T) {
				t.Helper()
				_, uErr := client.UpdateTimeToLive(t.Context(), &dynamodb.UpdateTimeToLiveInput{
					TableName: aws.String(tableName),
					TimeToLiveSpecification: &types.TimeToLiveSpecification{
						AttributeName: aws.String("ttl_attr"),
						Enabled:       aws.Bool(true),
					},
				})
				require.NoError(t, uErr)

				descOut, err := client.DescribeTimeToLive(t.Context(), &dynamodb.DescribeTimeToLiveInput{
					TableName: aws.String(tableName),
				})
				require.NoError(t, err)
				assert.Equal(t, types.TimeToLiveStatusEnabled, descOut.TimeToLiveDescription.TimeToLiveStatus)
				assert.Equal(t, "ttl_attr", *descOut.TimeToLiveDescription.AttributeName)
			},
		},
		{
			name: "TransactWriteAndGet",
			run: func(t *testing.T) {
				t.Helper()
				_, tErr := client.TransactWriteItems(t.Context(), &dynamodb.TransactWriteItemsInput{
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

				getOut, err := client.TransactGetItems(t.Context(), &dynamodb.TransactGetItemsInput{
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
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
