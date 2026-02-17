package dynamodb_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"Gopherstack/dynamodb"
	"Gopherstack/dynamodb/models"

	"github.com/stretchr/testify/require"
)

// BenchmarkQuery_ComplexModel benchmarks querying a complex data model with begins_with and projection.
func BenchmarkQuery_ComplexModel(b *testing.B) {
	counts := []int{100, 1000}
	for _, count := range counts {
		b.Run(fmt.Sprintf("Count_%d", count), func(b *testing.B) {
			db := setupComplexDB(b, count)

			input := models.QueryInput{
				TableName:              "ComplexBenchTable",
				KeyConditionExpression: "pk = :type AND begins_with(sk, :orgPrefix)",
				ExpressionAttributeValues: map[string]any{
					":type":      map[string]any{"S": "USER_PROFILE"},
					":orgPrefix": map[string]any{"S": "ORG#org-1#"},
				},
				ProjectionExpression: "sk, DeepData.Config.Theme, DeepData.Meta.Source, Tags",
			}
			sdkInput, err := models.ToSDKQueryInput(&input)
			require.NoError(b, err)

			b.ResetTimer()
			for range b.N {
				_, err = db.Query(context.Background(), sdkInput)
				require.NoError(b, err)
			}
		})
	}
}

// setupComplexDB creates a DB with complex nested items.
func setupComplexDB(b *testing.B, count int) *dynamodb.InMemoryDB {
	b.Helper()
	db := dynamodb.NewInMemoryDB()

	// Create Table
	createInput := models.CreateTableInput{
		TableName: "ComplexBenchTable",
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "pk", KeyType: "HASH"},
			{AttributeName: "sk", KeyType: "RANGE"},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
			{AttributeName: "sk", AttributeType: "S"},
		},
	}
	sdkCreate := models.ToSDKCreateTableInput(&createInput)
	_, err := db.CreateTable(context.Background(), sdkCreate)
	require.NoError(b, err)

	// Populate Data
	for i := range count {
		orgID := fmt.Sprintf("org-%d", i%10)
		userID := fmt.Sprintf("user-%d", i)
		sk := fmt.Sprintf("ORG#%s#USER#%s", orgID, userID)

		item := createComplexItem("USER_PROFILE", sk, orgID, userID)
		putInput := models.PutItemInput{
			TableName: "ComplexBenchTable",
			Item:      item,
		}
		sdkPut, err2 := models.ToSDKPutItemInput(&putInput)
		require.NoError(b, err2)
		_, err = db.PutItem(context.Background(), sdkPut)
		require.NoError(b, err)
	}

	// Add noise data
	for i := range count * 2 { // Reduced multiplier to keep benchmark realistic and fast
		item := createComplexItem(fmt.Sprintf("NOISE_%d", i%100), "sk", "org", "user")
		putInput := models.PutItemInput{
			TableName: "ComplexBenchTable",
			Item:      item,
		}
		sdkPut, err2 := models.ToSDKPutItemInput(&putInput)
		require.NoError(b, err2)
		_, err = db.PutItem(context.Background(), sdkPut)
		require.NoError(b, err)
	}

	return db
}

func createComplexItem(pk, sk, orgID, userID string) map[string]any {
	return map[string]any{
		"pk": map[string]any{"S": pk},
		"sk": map[string]any{"S": sk},
		"DeepData": map[string]any{"M": map[string]any{
			"ID":  map[string]any{"S": userID},
			"Org": map[string]any{"S": orgID},
			"Meta": map[string]any{"M": map[string]any{
				"Source": map[string]any{"S": "benchmark"},
				"Ver":    map[string]any{"N": "1"},
				"Extra":  map[string]any{"S": strings.Repeat("x", 100)},
			}},
			"Config": map[string]any{"M": map[string]any{
				"Theme": map[string]any{"S": "dark"},
				"Prefs": map[string]any{"M": map[string]any{
					"Email": map[string]any{"BOOL": true},
					"SMS":   map[string]any{"BOOL": false},
				}},
			}},
		}},
		"Tags": map[string]any{"L": []any{
			map[string]any{"S": "tag1"},
			map[string]any{"S": "tag2"},
			map[string]any{"S": "tag3"},
		}},
	}
}
