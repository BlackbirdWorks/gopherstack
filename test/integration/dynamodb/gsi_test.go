//go:build integration

package dynamodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGSI(t *testing.T) {
	t.Parallel()
	client := createDynamoDBClient(t)

	// Helper to create table with GSI
	createTableWithGSI := func(t *testing.T, tableName string, gsiName string, projectionType types.ProjectionType) {
		_, err := client.CreateTable(t.Context(), &dynamodb.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("gsiPK"), AttributeType: types.ScalarAttributeTypeS},
				// {AttributeName: aws.String("gsiSK"), AttributeType: types.ScalarAttributeTypeS}, // Optional
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String(gsiName),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("gsiPK"), KeyType: types.KeyTypeHash},
					},
					Projection: &types.Projection{
						ProjectionType: projectionType,
						NonKeyAttributes: func() []string {
							if projectionType == types.ProjectionTypeInclude {
								return []string{"extra"}
							}
							return nil
						}(),
					},
					ProvisionedThroughput: &types.ProvisionedThroughput{
						ReadCapacityUnits:  aws.Int64(5),
						WriteCapacityUnits: aws.Int64(5),
					},
				},
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
	}

	t.Run("Query_GSI_All", func(t *testing.T) {
		t.Parallel()
		tableName := "GSI_All_" + uuid.NewString()
		gsiName := "GSI_1"
		createTableWithGSI(t, tableName, gsiName, types.ProjectionTypeAll)

		// Put Items
		// Item 1: Has GSI PK
		_, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "item1"},
				"gsiPK": &types.AttributeValueMemberS{Value: "gsiKey1"},
				"data":  &types.AttributeValueMemberS{Value: "some data"},
			},
		})
		require.NoError(t, err)

		// Item 2: Has different GSI PK
		_, err = client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "item2"},
				"gsiPK": &types.AttributeValueMemberS{Value: "gsiKey2"},
				"data":  &types.AttributeValueMemberS{Value: "other data"},
			},
		})
		require.NoError(t, err)

		// Item 3: No GSI PK
		_, err = client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":   &types.AttributeValueMemberS{Value: "item3"},
				"data": &types.AttributeValueMemberS{Value: "no gsi"},
			},
		})
		require.NoError(t, err)

		// Query GSI
		out, err := client.Query(t.Context(), &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			IndexName:              aws.String(gsiName),
			KeyConditionExpression: aws.String("gsiPK = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":v": &types.AttributeValueMemberS{Value: "gsiKey1"},
			},
		})
		assert.NoError(t, err)
		assert.Len(t, out.Items, 1)
		assert.Equal(t, "item1", out.Items[0]["pk"].(*types.AttributeValueMemberS).Value)
		assert.Equal(t, "some data", out.Items[0]["data"].(*types.AttributeValueMemberS).Value) // ALL projection
	})

	t.Run("Query_GSI_KeysOnly", func(t *testing.T) {
		t.Parallel()
		tableName := "GSI_Keys_" + uuid.NewString()
		gsiName := "GSI_2"
		createTableWithGSI(t, tableName, gsiName, types.ProjectionTypeKeysOnly)

		_, err := client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "item1"},
				"gsiPK": &types.AttributeValueMemberS{Value: "gsiKey1"},
				"data":  &types.AttributeValueMemberS{Value: "hidden"},
			},
		})
		require.NoError(t, err)

		out, err := client.Query(t.Context(), &dynamodb.QueryInput{
			TableName:              aws.String(tableName),
			IndexName:              aws.String(gsiName),
			KeyConditionExpression: aws.String("gsiPK = :v"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":v": &types.AttributeValueMemberS{Value: "gsiKey1"},
			},
		})
		assert.NoError(t, err)
		assert.Len(t, out.Items, 1)
		assert.Equal(t, "item1", out.Items[0]["pk"].(*types.AttributeValueMemberS).Value)
		// data should be missing
		_, ok := out.Items[0]["data"]
		assert.False(t, ok, "data should not be projected")
	})

	t.Run("Scan_GSI_Sparse", func(t *testing.T) {
		t.Parallel()
		tableName := "GSI_Scan_" + uuid.NewString()
		gsiName := "GSI_3"
		createTableWithGSI(t, tableName, gsiName, types.ProjectionTypeAll)

		// 1 GSI item, 1 Non-GSI item
		client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk":    &types.AttributeValueMemberS{Value: "item1"},
				"gsiPK": &types.AttributeValueMemberS{Value: "val"},
			},
		})
		client.PutItem(t.Context(), &dynamodb.PutItemInput{
			TableName: aws.String(tableName),
			Item: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: "item2"},
			},
		})

		// Scan Table -> 2 items
		scanStart, _ := client.Scan(t.Context(), &dynamodb.ScanInput{TableName: aws.String(tableName)})
		assert.Len(t, scanStart.Items, 2)

		// Scan GSI -> 1 item
		scanGSI, err := client.Scan(t.Context(), &dynamodb.ScanInput{
			TableName: aws.String(tableName),
			IndexName: aws.String(gsiName),
		})
		assert.NoError(t, err)
		assert.Len(t, scanGSI.Items, 1)
		assert.Equal(t, "item1", scanGSI.Items[0]["pk"].(*types.AttributeValueMemberS).Value)
	})
}
