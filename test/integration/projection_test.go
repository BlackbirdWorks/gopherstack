//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestProjectionExpressions(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)

	type testCase struct {
		name      string
		setup     func(t *testing.T, ctx context.Context, tableName string)
		operation func(t *testing.T, ctx context.Context, tableName string) (interface{}, error)
		verify    func(t *testing.T, result interface{}, err error)
	}

	tests := []testCase{
		{
			name: "GetItem_Projection_Simple",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":    &types.AttributeValueMemberS{Value: "item1"},
						"info":  &types.AttributeValueMemberS{Value: "some info"},
						"extra": &types.AttributeValueMemberS{Value: "extra data"},
					},
				})
				assert.NoError(t, err)
			},
			operation: func(t *testing.T, ctx context.Context, tableName string) (interface{}, error) {
				return client.GetItem(ctx, &dynamodb.GetItemInput{
					TableName: aws.String(tableName),
					Key: map[string]types.AttributeValue{
						"pk": &types.AttributeValueMemberS{Value: "item1"},
					},
					ProjectionExpression: aws.String("pk, info"),
				})
			},
			verify: func(t *testing.T, result interface{}, err error) {
				assert.NoError(t, err)
				resp := result.(*dynamodb.GetItemOutput)
				assert.NotNil(t, resp.Item)
				assert.Equal(t, "item1", resp.Item["pk"].(*types.AttributeValueMemberS).Value)
				assert.Equal(t, "some info", resp.Item["info"].(*types.AttributeValueMemberS).Value)
				_, ok := resp.Item["extra"]
				assert.False(t, ok, "extra attribute should not be present")
			},
		},
		{
			name: "Scan_Projection_WithAttributeNames",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
				_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item: map[string]types.AttributeValue{
						"pk":   &types.AttributeValueMemberS{Value: "item1"},
						"data": &types.AttributeValueMemberS{Value: "value1"},
						"meta": &types.AttributeValueMemberS{Value: "meta1"},
					},
				})
				assert.NoError(t, err)
			},
			operation: func(t *testing.T, ctx context.Context, tableName string) (interface{}, error) {
				return client.Scan(ctx, &dynamodb.ScanInput{
					TableName:            aws.String(tableName),
					ProjectionExpression: aws.String("#d, meta"),
					ExpressionAttributeNames: map[string]string{
						"#d": "data",
					},
				})
			},
			verify: func(t *testing.T, result interface{}, err error) {
				assert.NoError(t, err)
				resp := result.(*dynamodb.ScanOutput)
				assert.Len(t, resp.Items, 1)
				item := resp.Items[0]
				assert.Equal(t, "value1", item["data"].(*types.AttributeValueMemberS).Value)
				assert.Equal(t, "meta1", item["meta"].(*types.AttributeValueMemberS).Value)
				_, ok := item["pk"]
				assert.False(t, ok, "pk should not be present")
			},
		},
		{
			name: "Query_Projection_Nested",
			setup: func(t *testing.T, ctx context.Context, tableName string) {
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
				assert.NoError(t, err)
			},
			operation: func(t *testing.T, ctx context.Context, tableName string) (interface{}, error) {
				return client.Query(ctx, &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "user1"},
					},
					ProjectionExpression: aws.String("profile.name, status"),
				})
			},
			verify: func(t *testing.T, result interface{}, err error) {
				assert.NoError(t, err)
				resp := result.(*dynamodb.QueryOutput)
				assert.Len(t, resp.Items, 1)
				item := resp.Items[0]

				// Verify status
				assert.Equal(t, "active", item["status"].(*types.AttributeValueMemberS).Value)

				// Verify nested profile.name
				profile, ok := item["profile"].(*types.AttributeValueMemberM)
				assert.True(t, ok)
				assert.NotNil(t, profile.Value["name"])
				assert.Equal(t, "Andrew", profile.Value["name"].(*types.AttributeValueMemberS).Value)

				// Verify profile.age is MISSING
				_, hasAge := profile.Value["age"]
				assert.False(t, hasAge, "profile.age should be missing")
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Create dedicated table for each test to allow parallel execution without conflict
			tableName := createTable(t, client)
			tc.setup(t, context.TODO(), tableName)
			res, err := tc.operation(t, context.TODO(), tableName)
			tc.verify(t, res, err)
			// Cleanup happens automatically by defer deleteTable (if implemented in createTable helper) or relying on container cleanup
		})
	}
}

func createTable(t *testing.T, client *dynamodb.Client) string {
	t.Helper()
	tableName := "ProjectionTable-" + uuid.NewString()
	ctx := context.TODO()

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
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Cleanup(func() {
		client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	})

	// Wait for table to be active (mock is instant, but good practice)
	// time.Sleep(10 * time.Millisecond)

	return tableName
}
