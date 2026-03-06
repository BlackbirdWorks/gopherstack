package dynamodb_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"

	"github.com/stretchr/testify/require"
)

func BenchmarkGetItem(b *testing.B) {
	sizes := []int{10000, 100000}
	for _, size := range sizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			db := setupDBWithItems(b, size)
			input := models.GetItemInput{
				TableName: "BenchTable",
				Key:       map[string]any{"id": map[string]any{"S": strconv.Itoa(size / 2)}},
			}
			sdkInput, _ := models.ToSDKGetItemInput(&input)

			b.ResetTimer()
			for range b.N {
				_, _ = db.GetItem(context.Background(), sdkInput)
			}
		})
	}
}

func BenchmarkQuery(b *testing.B) {
	b.Run("WithIndex_10k", func(b *testing.B) {
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
			_, _ = db.Query(context.Background(), sdkInput)
		}
	})

	b.Run("WithFilter_10k", func(b *testing.B) {
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
			_, _ = db.Query(context.Background(), sdkInput)
		}
	})
}

func BenchmarkScan(b *testing.B) {
	b.Run("100k", func(b *testing.B) {
		db := setupDBWithItems(b, 100000)
		input := models.ScanInput{
			TableName: "BenchTable",
		}
		sdkInput, _ := models.ToSDKScanInput(&input)

		b.ResetTimer()
		for range b.N {
			_, _ = db.Scan(context.Background(), sdkInput)
		}
	})
}

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
		_, _ = db.PutItem(context.Background(), sdkInput)
	}
}

func BenchmarkConcurrent(b *testing.B) {
	b.Run("Reads_10k", func(b *testing.B) {
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
				_, _ = db.GetItem(context.Background(), sdkInput)
				i++
			}
		})
	})

	b.Run("Writes", func(b *testing.B) {
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
				_, _ = db.PutItem(context.Background(), sdkInput)
				i++
			}
		})
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

	_, err := db.CreateTable(context.Background(), sdkInput)
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
		_, err = db.PutItem(context.Background(), sdkInput)
		require.NoError(b, err)
	}

	return db
}
