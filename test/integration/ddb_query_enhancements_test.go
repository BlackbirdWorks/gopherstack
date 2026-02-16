package integration_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_DDB_QueryEnhancements(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)
	client := createDynamoDBClient(t)

	createTableWithItems := func(t *testing.T, pk string, count int) string {
		t.Helper()
		tableName := "QueryEnhance_" + uuid.NewString()
		_, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("sk"), AttributeType: types.ScalarAttributeTypeN},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
				{AttributeName: aws.String("sk"), KeyType: types.KeyTypeRange},
			},
			ProvisionedThroughput: &types.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(5),
				WriteCapacityUnits: aws.Int64(5),
			},
		})
		require.NoError(t, err)

		t.Cleanup(func() {
			client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
		})
		time.Sleep(10 * time.Millisecond)

		for i := 1; i <= count; i++ {
			_, pErr := client.PutItem(t.Context(), &dynamodb.PutItemInput{
				TableName: aws.String(tableName),
				Item: map[string]types.AttributeValue{
					"pk": &types.AttributeValueMemberS{Value: pk},
					"sk": &types.AttributeValueMemberN{Value: strconv.Itoa(i)},
				},
			})
			require.NoError(t, pErr)
		}

		return tableName
	}

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "SortKey_Forward",
			run: func(t *testing.T) {
				t.Helper()
				tableName := createTableWithItems(t, "A", 5)

				out, err := client.Query(t.Context(), &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "A"},
					},
				})
				require.NoError(t, err)
				assert.Len(t, out.Items, 5)
				assert.Equal(t, "1", out.Items[0]["sk"].(*types.AttributeValueMemberN).Value)
				assert.Equal(t, "5", out.Items[4]["sk"].(*types.AttributeValueMemberN).Value)
			},
		},
		{
			name: "SortKey_Reverse",
			run: func(t *testing.T) {
				t.Helper()
				tableName := createTableWithItems(t, "A", 5)

				out, err := client.Query(t.Context(), &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "A"},
					},
					ScanIndexForward: aws.Bool(false),
				})
				require.NoError(t, err)
				assert.Len(t, out.Items, 5)
				assert.Equal(t, "5", out.Items[0]["sk"].(*types.AttributeValueMemberN).Value)
				assert.Equal(t, "1", out.Items[4]["sk"].(*types.AttributeValueMemberN).Value)
			},
		},
		{
			name: "SortKey_GreaterThan",
			run: func(t *testing.T) {
				t.Helper()
				tableName := createTableWithItems(t, "B", 5)

				out, err := client.Query(t.Context(), &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("pk = :pk AND sk > :v"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "B"},
						":v":  &types.AttributeValueMemberN{Value: "3"},
					},
				})
				require.NoError(t, err)
				assert.Len(t, out.Items, 2)
				assert.Equal(t, "4", out.Items[0]["sk"].(*types.AttributeValueMemberN).Value)
			},
		},
		{
			name: "SortKey_LessThanOrEqual",
			run: func(t *testing.T) {
				t.Helper()
				tableName := createTableWithItems(t, "B", 5)

				out, err := client.Query(t.Context(), &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("pk = :pk AND sk <= :v"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "B"},
						":v":  &types.AttributeValueMemberN{Value: "2"},
					},
				})
				require.NoError(t, err)
				assert.Len(t, out.Items, 2)
			},
		},
		{
			name: "Pagination",
			run: func(t *testing.T) {
				t.Helper()
				tableName := createTableWithItems(t, "C", 10)

				out1, err := client.Query(t.Context(), &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "C"},
					},
					Limit: aws.Int32(3),
				})
				require.NoError(t, err)
				assert.Len(t, out1.Items, 3)
				assert.NotNil(t, out1.LastEvaluatedKey)
				assert.Equal(t, "3", out1.Items[2]["sk"].(*types.AttributeValueMemberN).Value)

				out2, err := client.Query(t.Context(), &dynamodb.QueryInput{
					TableName:              aws.String(tableName),
					KeyConditionExpression: aws.String("pk = :pk"),
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":pk": &types.AttributeValueMemberS{Value: "C"},
					},
					Limit:             aws.Int32(3),
					ExclusiveStartKey: out1.LastEvaluatedKey,
				})
				require.NoError(t, err)
				assert.Len(t, out2.Items, 3)
				assert.Equal(t, "4", out2.Items[0]["sk"].(*types.AttributeValueMemberN).Value)
				assert.Equal(t, "6", out2.Items[2]["sk"].(*types.AttributeValueMemberN).Value)
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
