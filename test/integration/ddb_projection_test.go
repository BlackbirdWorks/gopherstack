package integration_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_ProjectionExpressions(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	type testCase struct {
		setup     func(t *testing.T, ctx context.Context, tableName string)
		operation func(t *testing.T, ctx context.Context, tableName string) (any, error)
		verify    func(t *testing.T, result any, err error)
		name      string
	}

	tests := []testCase{
		{
			name: "GetItem_Projection_Simple",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				t.Helper()
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":    &types.AttributeValueMemberS{Value: "item1"},
						"info":  &types.AttributeValueMemberS{Value: "some info"},
						"extra": &types.AttributeValueMemberS{Value: "extra data"},
					},
				})
				require.NoError(t, err)
			},
			operation: func(_ *testing.T, ctx context.Context, tableName string) (any, error) {
				return client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(tableName),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
					ProjectionExpression: aws.String("pk, info"),
				})
			},
			verify: func(t *testing.T, result any, err error) {
				t.Helper()
				require.NoError(t, err)
				resp := result.(*dynamodb.GetItemOutput)
				require.NotNil(t, resp.Item)
				AssertItem(t, resp.Item, map[string]any{
					"pk":   "item1",
					"info": "some info",
				})
			},
		},
		{
			name: "Scan_Projection_WithAttributeNames",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				t.Helper()
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":   &types.AttributeValueMemberS{Value: "item1"},
						"data": &types.AttributeValueMemberS{Value: "value1"},
						"meta": &types.AttributeValueMemberS{Value: "meta1"},
					},
				})
				require.NoError(t, err)
			},
			operation: func(_ *testing.T, ctx context.Context, tableName string) (any, error) {
				return client.Scan(ctx, &dynamodb.ScanInput{
					TableName:            aws.String(tableName),
					ProjectionExpression: aws.String("#d, meta"),
					ExpressionAttributeNames: map[string]string{
						"#d": "data",
					},
				})
			},
			verify: func(t *testing.T, result any, err error) {
				t.Helper()
				require.NoError(t, err)
				resp := result.(*dynamodb.ScanOutput)
				require.Len(t, resp.Items, 1)
				AssertItem(t, resp.Items[0], map[string]any{
					"data": "value1",
					"meta": "meta1",
				})
			},
		},
		{
			name: "Query_Projection_Nested",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				t.Helper()
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "user1"},
						"profile": &types.AttributeValueMemberM{
							Value: map[string]types.AttributeValue{
								"name": &types.AttributeValueMemberS{Value: "Andrew"},
								"age":  &types.AttributeValueMemberN{Value: "30"},
							},
						},
						"status": &types.AttributeValueMemberS{Value: "active"},
					},
				})
				require.NoError(t, err)
			},
			operation: func(_ *testing.T, ctx context.Context, tableName string) (any, error) {
				return client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "user1"},
					},
					ProjectionExpression: aws.String("profile.name, status"),
				})
			},
			verify: func(t *testing.T, result any, err error) {
				t.Helper()
				require.NoError(t, err)
				resp := result.(*dynamodb.QueryOutput)
				require.Len(t, resp.Items, 1)
				AssertItem(t, resp.Items[0], map[string]any{
					"status": "active",
					"profile": map[string]any{
						"name": "Andrew",
					},
				})
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tableName := createTable(t, client)
			ctx := t.Context()
			tc.setup(t, ctx, tableName)
			res, err := tc.operation(t, ctx, tableName)
			tc.verify(t, res, err)
		})
	}
}

func createTable(t *testing.T, client *dynamodb.Client) string {
	t.Helper()
	tableName := "ProjectionTable-" + uuid.NewString()

	_, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
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
		client.DeleteTable(
			context.Background(),
			&dynamodb.DeleteTableInput{TableName: aws.String(tableName)},
		)
	})

	return tableName
}
