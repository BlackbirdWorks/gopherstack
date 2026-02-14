package dynamodb_test

import (
	"encoding/json"
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
	// Query: pk="USER_PROFILE", begins_with(sk, "ORG#org-1#")
	// Projection: sk, DeepData.Config.Theme, DeepData.Meta.Source, Tags
	input := dynamodb.QueryInput{
		TableName:              "ComplexBenchTable",
		KeyConditionExpression: "pk = :type AND begins_with(sk, :orgPrefix)",
		ExpressionAttributeValues: map[string]any{
			":type":      map[string]any{"S": "USER_PROFILE"},
			":orgPrefix": map[string]any{"S": "ORG#org-1#"},
		},
		ProjectionExpression: "sk, DeepData.Config.Theme, DeepData.Meta.Source, Tags",
	}
	body, _ := json.Marshal(input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.Query(body)
	}
}

// setupComplexDB creates a DB with complex nested items.
func setupComplexDB(count int) *dynamodb.InMemoryDB {
	db := dynamodb.NewInMemoryDB()

	// Create Table
	createInput := dynamodb.CreateTableInput{
		TableName: "ComplexBenchTable",
		KeySchema: []dynamodb.KeySchemaElement{
			{AttributeName: "pk", KeyType: dynamodb.KeyTypeHash},
			{AttributeName: "sk", KeyType: dynamodb.KeyTypeRange},
		},
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{AttributeName: "pk", AttributeType: "S"},
			{AttributeName: "sk", AttributeType: "S"},
		},
	}
	createBody, _ := json.Marshal(createInput)
	_, _ = db.CreateTable(createBody)

	// Populate Data
	// 10 Orgs, each with count/10 users (Target Data)
	for i := range count {
		orgID := fmt.Sprintf("org-%d", i%10) // 10 Orgs
		userID := fmt.Sprintf("user-%d", i)
		sk := fmt.Sprintf("ORG#%s#USER#%s", orgID, userID)

		// Large nested item
		item := createComplexItem("USER_PROFILE", sk, orgID, userID)

		putInput := dynamodb.PutItemInput{
			TableName: "ComplexBenchTable",
			Item:      item,
		}
		putBody, _ := json.Marshal(putInput)
		_, _ = db.PutItem(putBody)
	}

	// Add noise data (different PKs) - 9x the count
	for i := range count * 9 {
		item := createComplexItem(fmt.Sprintf("NOISE_%d", i%100), "sk", "org", "user")
		putInput := dynamodb.PutItemInput{
			TableName: "ComplexBenchTable",
			Item:      item,
		}
		putBody, _ := json.Marshal(putInput)
		_, _ = db.PutItem(putBody)
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
