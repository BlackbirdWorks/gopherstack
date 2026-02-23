package integration_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func createScanTable(t *testing.T, client *dynamodb.Client, tableName string) {
	t.Helper()
	ctx := t.Context()

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
		client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{ //nolint:errcheck
			TableName: aws.String(tableName),
		})
	})
}

func TestIntegration_DDB_Scan(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	tests := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "basic scan returns all items",
			run: func(t *testing.T) {
				t.Helper()
				client := createDynamoDBClient(t)
				tableName := "ScanBasic-" + uuid.NewString()
				createScanTable(t, client, tableName)
				ctx := t.Context()

				for i := range 10 {
					_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"pk":    &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
							"value": &types.AttributeValueMemberN{Value: fmt.Sprintf("%d", i)},
						},
					})
					require.NoError(t, err)
				}

				out, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(tableName),
				})
				require.NoError(t, err)
				assert.Equal(t, int32(10), out.Count)
				assert.Len(t, out.Items, 10)
			},
		},
		{
			name: "scan with filter expression",
			run: func(t *testing.T) {
				t.Helper()
				client := createDynamoDBClient(t)
				tableName := "ScanFilter-" + uuid.NewString()
				createScanTable(t, client, tableName)
				ctx := t.Context()

				// Put 5 active and 5 inactive items
				for i := range 10 {
					status := "active"
					if i%2 == 0 {
						status = "inactive"
					}
					_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"pk":     &types.AttributeValueMemberS{Value: fmt.Sprintf("item-%d", i)},
							"status": &types.AttributeValueMemberS{Value: status},
						},
					})
					require.NoError(t, err)
				}

				out, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:        aws.String(tableName),
					FilterExpression: aws.String("attribute_not_exists(deleted) AND #s = :s"),
					ExpressionAttributeNames: map[string]string{
						"#s": "status",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":s": &types.AttributeValueMemberS{Value: "active"},
					},
				})
				require.NoError(t, err)
				assert.Equal(t, int32(5), out.Count)
			},
		},
		{
			name: "scan with limit and pagination",
			run: func(t *testing.T) {
				t.Helper()
				client := createDynamoDBClient(t)
				tableName := "ScanPage-" + uuid.NewString()
				createScanTable(t, client, tableName)
				ctx := t.Context()

				for i := range 10 {
					_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("page-item-%d", i)},
						},
					})
					require.NoError(t, err)
				}

				// First page
				firstOut, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName: aws.String(tableName),
					Limit:     aws.Int32(3),
				})
				require.NoError(t, err)
				assert.LessOrEqual(t, int(firstOut.Count), 3)
				require.NotNil(t, firstOut.LastEvaluatedKey, "expected pagination key for first page")

				// Continue scanning until all items retrieved
				total := int(firstOut.Count)
				lastKey := firstOut.LastEvaluatedKey
				for lastKey != nil {
					nextOut, nextErr := client.Scan(ctx, &dynamodb.ScanInput{
						TableName:         aws.String(tableName),
						Limit:             aws.Int32(3),
						ExclusiveStartKey: lastKey,
					})
					require.NoError(t, nextErr)
					total += int(nextOut.Count)
					lastKey = nextOut.LastEvaluatedKey
				}

				assert.Equal(t, 10, total)
			},
		},
		{
			name: "scan with projection expression",
			run: func(t *testing.T) {
				t.Helper()
				client := createDynamoDBClient(t)
				tableName := "ScanProj-" + uuid.NewString()
				createScanTable(t, client, tableName)
				ctx := t.Context()

				for i := range 5 {
					_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"pk":     &types.AttributeValueMemberS{Value: fmt.Sprintf("proj-item-%d", i)},
							"name":   &types.AttributeValueMemberS{Value: fmt.Sprintf("name-%d", i)},
							"secret": &types.AttributeValueMemberS{Value: "should-not-appear"},
						},
					})
					require.NoError(t, err)
				}

				out, err := client.Scan(ctx, &dynamodb.ScanInput{
					TableName:            aws.String(tableName),
					ProjectionExpression: aws.String("pk, #n"),
					ExpressionAttributeNames: map[string]string{
						"#n": "name",
					},
				})
				require.NoError(t, err)
				assert.Len(t, out.Items, 5)

				for _, item := range out.Items {
					assert.Contains(t, item, "pk")
					assert.Contains(t, item, "name")
					assert.NotContains(t, item, "secret")
				}
			},
		},
		{
			name: "parallel scan across segments",
			run: func(t *testing.T) {
				t.Helper()
				client := createDynamoDBClient(t)
				tableName := "ScanParallel-" + uuid.NewString()
				createScanTable(t, client, tableName)
				ctx := t.Context()

				for i := range 10 {
					_, err := client.PutItem(ctx, &dynamodb.PutItemInput{
						TableName: aws.String(tableName),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: fmt.Sprintf("parallel-item-%d", i)},
						},
					})
					require.NoError(t, err)
				}

				totalSegments := int32(2)
				totalItems := 0

				for seg := int32(0); seg < totalSegments; seg++ {
					var lastKey map[string]types.AttributeValue
					for {
						out, err := client.Scan(ctx, &dynamodb.ScanInput{
							TableName:         aws.String(tableName),
							Segment:           aws.Int32(seg),
							TotalSegments:     aws.Int32(totalSegments),
							ExclusiveStartKey: lastKey,
						})
						require.NoError(t, err)
						totalItems += int(out.Count)
						lastKey = out.LastEvaluatedKey
						if lastKey == nil {
							break
						}
					}
				}

				assert.Equal(t, 10, totalItems)
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
