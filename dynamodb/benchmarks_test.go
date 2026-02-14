package dynamodb_test

import (
	"encoding/json"
	"strconv"
	"testing"

	"Gopherstack/dynamodb"
)

// BenchmarkGetItem_10k benchmarks point lookups with 10k items.
func BenchmarkGetItem_10k(b *testing.B) {
	db := setupDBWithItems(10000)
	input := dynamodb.GetItemInput{
		TableName: "BenchTable",
		Key:       map[string]any{"id": map[string]any{"S": "5000"}}, // middle item.
	}
	body, _ := json.Marshal(input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.GetItem(body)
	}
}

// BenchmarkGetItem_100k benchmarks point lookups with 100k items.
func BenchmarkGetItem_100k(b *testing.B) {
	db := setupDBWithItems(100000)
	input := dynamodb.GetItemInput{
		TableName: "BenchTable",
		Key:       map[string]any{"id": map[string]any{"S": "50000"}},
	}
	body, _ := json.Marshal(input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.GetItem(body)
	}
}

// BenchmarkQuery_WithIndex_10k benchmarks keyed queries with 10k items.
func BenchmarkQuery_WithIndex_10k(b *testing.B) {
	db := setupDBWithItems(10000)
	input := dynamodb.QueryInput{
		TableName:              "BenchTable",
		KeyConditionExpression: "id = :id",
		ExpressionAttributeValues: map[string]any{
			":id": map[string]any{"S": "5000"},
		},
	}
	body, _ := json.Marshal(input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.Query(body)
	}
}

// BenchmarkQuery_WithFilter_10k benchmarks queries with filter expression.
func BenchmarkQuery_WithFilter_10k(b *testing.B) {
	db := setupDBWithItems(10000)
	input := dynamodb.QueryInput{
		TableName:              "BenchTable",
		KeyConditionExpression: "id = :id",
		FilterExpression:       "val > :val",
		ExpressionAttributeValues: map[string]any{
			":id":  map[string]any{"S": "5000"},
			":val": map[string]any{"N": "1000"},
		},
	}
	body, _ := json.Marshal(input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.Query(body)
	}
}

// BenchmarkScan_100k benchmarks full table scan with 100k items.
func BenchmarkScan_100k(b *testing.B) {
	db := setupDBWithItems(100000)
	input := dynamodb.ScanInput{
		TableName: "BenchTable",
	}
	body, _ := json.Marshal(input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.Scan(body)
	}
}

// BenchmarkPutItem_WithIndex benchmarks item insertion with index maintenance.
func BenchmarkPutItem_WithIndex(b *testing.B) {
	db := setupEmptyTable()

	b.ResetTimer()
	for i := range b.N {
		input := dynamodb.PutItemInput{
			TableName: "BenchTable",
			Item: map[string]any{
				"id":  map[string]any{"S": strconv.Itoa(i)},
				"val": map[string]any{"N": strconv.Itoa(i * 10)},
			},
		}
		body, _ := json.Marshal(input)
		_, _ = db.PutItem(body)
	}
}

// BenchmarkConcurrentReads benchmarks concurrent read operations.
func BenchmarkConcurrentReads(b *testing.B) {
	db := setupDBWithItems(10000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			input := dynamodb.GetItemInput{
				TableName: "BenchTable",
				Key:       map[string]any{"id": map[string]any{"S": strconv.Itoa(i % 10000)}},
			}
			body, _ := json.Marshal(input)
			_, _ = db.GetItem(body)
			i++
		}
	})
}

// BenchmarkConcurrentWrites benchmarks concurrent write operations.
func BenchmarkConcurrentWrites(b *testing.B) {
	db := setupEmptyTable()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			input := dynamodb.PutItemInput{
				TableName: "BenchTable",
				Item: map[string]any{
					"id":  map[string]any{"S": "item-" + strconv.Itoa(i)},
					"val": map[string]any{"N": strconv.Itoa(i)},
				},
			}
			body, _ := json.Marshal(input)
			_, _ = db.PutItem(body)
			i++
		}
	})
}

// Helper functions

func setupEmptyTable() *dynamodb.InMemoryDB {
	db := dynamodb.NewInMemoryDB()
	createInput := dynamodb.CreateTableInput{
		TableName: "BenchTable",
		KeySchema: []dynamodb.KeySchemaElement{
			{AttributeName: "id", KeyType: dynamodb.KeyTypeHash},
		},
		AttributeDefinitions: []dynamodb.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
		},
	}
	body, _ := json.Marshal(createInput)
	_, _ = db.CreateTable(body)

	return db
}

func setupDBWithItems(count int) *dynamodb.InMemoryDB {
	db := setupEmptyTable()

	// Batch insert items
	for i := range count {
		input := dynamodb.PutItemInput{
			TableName: "BenchTable",
			Item: map[string]any{
				"id":  map[string]any{"S": strconv.Itoa(i)},
				"val": map[string]any{"N": strconv.Itoa(i * 10)},
			},
		}
		body, _ := json.Marshal(input)
		_, _ = db.PutItem(body)
	}

	return db
}
