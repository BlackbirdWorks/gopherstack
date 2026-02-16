package dynamodb_test

import (
	"Gopherstack/dynamodb/models"
	"strconv"
	"testing"

	"Gopherstack/dynamodb"

	"github.com/stretchr/testify/require"
)

// BenchmarkGetItem_10k benchmarks point lookups with 10k items.
func BenchmarkGetItem_10k(b *testing.B) {
	db := setupDBWithItems(b, 10000)
	input := models.GetItemInput{
		TableName: "BenchTable",
		Key:       map[string]any{"id": map[string]any{"S": "5000"}}, // middle item.
	}
	sdkInput, _ := models.ToSDKGetItemInput(&input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.GetItem(sdkInput)
	}
}

// BenchmarkGetItem_100k benchmarks point lookups with 100k items.
func BenchmarkGetItem_100k(b *testing.B) {
	db := setupDBWithItems(b, 100000)
	input := models.GetItemInput{
		TableName: "BenchTable",
		Key:       map[string]any{"id": map[string]any{"S": "50000"}},
	}
	sdkInput, _ := models.ToSDKGetItemInput(&input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.GetItem(sdkInput)
	}
}

// BenchmarkQuery_WithIndex_10k benchmarks keyed queries with 10k items.
func BenchmarkQuery_WithIndex_10k(b *testing.B) {
	db := setupDBWithItems(b, 10000)
	input := models.QueryInput{
		TableName:              "BenchTable",
		KeyConditionExpression: "id = :id",
		ExpressionAttributeValues: map[string]any{
			":id": map[string]any{"S": "5000"},
		},
	}
	sdkInput, _ := models.ToSDKQueryInput(&input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.Query(sdkInput)
	}
}

// BenchmarkQuery_WithFilter_10k benchmarks queries with filter expression.
func BenchmarkQuery_WithFilter_10k(b *testing.B) {
	db := setupDBWithItems(b, 10000)
	input := models.QueryInput{
		TableName:              "BenchTable",
		KeyConditionExpression: "id = :id",
		FilterExpression:       "val > :val",
		ExpressionAttributeValues: map[string]any{
			":id":  map[string]any{"S": "5000"},
			":val": map[string]any{"N": "1000"},
		},
	}
	sdkInput, _ := models.ToSDKQueryInput(&input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.Query(sdkInput)
	}
}

// BenchmarkScan_100k benchmarks full table scan with 100k items.
func BenchmarkScan_100k(b *testing.B) {
	db := setupDBWithItems(b, 100000)
	input := models.ScanInput{
		TableName: "BenchTable",
	}
	sdkInput, _ := models.ToSDKScanInput(&input)

	b.ResetTimer()
	for range b.N {
		_, _ = db.Scan(sdkInput)
	}
}

// BenchmarkPutItem_WithIndex benchmarks item insertion with index maintenance.
func BenchmarkPutItem_WithIndex(b *testing.B) {
	db := setupEmptyTable(b)

	b.ResetTimer()
	for i := range b.N {
		input := models.PutItemInput{
			TableName: "BenchTable",
			Item: map[string]any{
				"id":  map[string]any{"S": strconv.Itoa(i)},
				"val": map[string]any{"N": strconv.Itoa(i * 10)},
			},
		}
		sdkInput, _ := models.ToSDKPutItemInput(&input)
		_, _ = db.PutItem(sdkInput)
	}
}

// BenchmarkConcurrentReads benchmarks concurrent read operations.
func BenchmarkConcurrentReads(b *testing.B) {
	db := setupDBWithItems(b, 10000)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			input := models.GetItemInput{
				TableName: "BenchTable",
				Key:       map[string]any{"id": map[string]any{"S": strconv.Itoa(i % 10000)}},
			}
			sdkInput, _ := models.ToSDKGetItemInput(&input)
			_, _ = db.GetItem(sdkInput)
			i++
		}
	})
}

// BenchmarkConcurrentWrites benchmarks concurrent write operations.
func BenchmarkConcurrentWrites(b *testing.B) {
	db := setupEmptyTable(b)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			input := models.PutItemInput{
				TableName: "BenchTable",
				Item: map[string]any{
					"id":  map[string]any{"S": "item-" + strconv.Itoa(i)},
					"val": map[string]any{"N": strconv.Itoa(i)},
				},
			}
			sdkInput, _ := models.ToSDKPutItemInput(&input)
			_, _ = db.PutItem(sdkInput)
			i++
		}
	})
}

// Helper functions

func setupEmptyTable(b *testing.B) *dynamodb.InMemoryDB {
	b.Helper()
	db := dynamodb.NewInMemoryDB()
	createInput := models.CreateTableInput{
		TableName: "BenchTable",
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "id", KeyType: models.KeyTypeHash},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
		},
	}
	sdkInput := models.ToSDKCreateTableInput(&createInput)

	_, err := db.CreateTable(sdkInput)
	require.NoError(b, err)

	return db
}

func setupDBWithItems(b *testing.B, count int) *dynamodb.InMemoryDB {
	b.Helper()
	db := setupEmptyTable(b)

	// Batch insert items
	for i := range count {
		input := models.PutItemInput{
			TableName: "BenchTable",
			Item: map[string]any{
				"id":  map[string]any{"S": strconv.Itoa(i)},
				"val": map[string]any{"N": strconv.Itoa(i * 10)},
			},
		}
		sdkInput, err := models.ToSDKPutItemInput(&input)
		require.NoError(b, err)
		_, err = db.PutItem(sdkInput)
		require.NoError(b, err)
	}

	return db
}
