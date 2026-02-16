package dynamodb_test

import (
	"Gopherstack/dynamodb/models"
	"fmt"
	"strings"
	"testing"

	"Gopherstack/dynamodb"
)

// BenchmarkQuery_ComplexModel benchmarks querying a complex data model with begins_with and projection.
func BenchmarkQuery_ComplexModel(b *testing.B) {
	// Setup DB with 1000 complex items
	db := setupComplexDB(1000)

	// Prepare Query Input
	input := models.QueryInput{
		TableName:              "ComplexBenchTable",
		KeyConditionExpression: "pk = :type AND begins_with(sk, :orgPrefix)",
		ExpressionAttributeValues: map[string]any{
			":type":      map[string]any{"S": "USER_PROFILE"},
			":orgPrefix": map[string]any{"S": "ORG#org-1#"},
		},
		ProjectionExpression: "sk, DeepData.Config.Theme, DeepData.Meta.Source, Tags",
	}

	b.ResetTimer()
	sdkInput, _ := models.ToSDKQueryInput(&input)

	for range b.N {
		_, _ = db.Query(sdkInput)
	}
}

// setupComplexDB creates a DB with complex nested items.
func setupComplexDB(count int) *dynamodb.InMemoryDB {
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
	_, _ = db.CreateTable(models.ToSDKCreateTableInput(&createInput))

	// Populate Data
	// 10 Orgs, each with count/10 users (Target Data)
	for i := range count {
		orgID := fmt.Sprintf("org-%d", i%10) // 10 Orgs
		userID := fmt.Sprintf("user-%d", i)
		sk := fmt.Sprintf("ORG#%s#USER#%s", orgID, userID)

		// Large nested item
		item := createComplexItem("USER_PROFILE", sk, orgID, userID)

		putInput := models.PutItemInput{
			TableName: "ComplexBenchTable",
			Item:      item,
		}
		sdkPut, _ := models.ToSDKPutItemInput(&putInput)
		_, _ = db.PutItem(sdkPut)
	}

	// Add noise data (different PKs) - 9x the count
	for i := range count * 9 {
		// Use simple map for noise to save memory in benchmark if needed,
		// but structure kept same for consistency
		item := createComplexItem(fmt.Sprintf("NOISE_%d", i%100), "sk", "org", "user")
		putInput := models.PutItemInput{
			TableName: "ComplexBenchTable",
			Item:      item,
		}
		sdkPut, _ := models.ToSDKPutItemInput(&putInput)
		_, _ = db.PutItem(sdkPut)
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
				"Extra":  map[string]any{"S": strings.Repeat("x", 100)}, // Payload
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
