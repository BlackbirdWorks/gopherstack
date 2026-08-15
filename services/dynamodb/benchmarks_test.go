package dynamodb_test

import (
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
				_, _ = db.GetItem(b.Context(), sdkInput)
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
			_, _ = db.Query(b.Context(), sdkInput)
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
			_, _ = db.Query(b.Context(), sdkInput)
		}
	})
}

// BenchmarkQuery_GSI measures Query against a GSI key condition. There is no
// per-GSI index structure (services/dynamodb/store.go's Table only maintains
// pkIndex/pkskIndex for the base table); filterCandidatesForKeyCondition in
// item_ops_query.go only calls tryFilterUsingAuthoritativeIndex when
// input.IndexName == "", so a GSI query always falls through to
// filterCandidatesScan, an O(table size) linear scan regardless of how
// selective the GSI key condition is.
func BenchmarkQuery_GSI(b *testing.B) {
	sizes := []int{10000, 100000}
	for _, size := range sizes {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			db := setupDBWithGSI(b, size)
			input := models.QueryInput{
				TableName:              "BenchTable",
				IndexName:              "gsi1",
				KeyConditionExpression: "gsipk = :gsipk",
				ExpressionAttributeValues: map[string]any{
					":gsipk": map[string]any{"S": strconv.Itoa(size / 2)},
				},
			}
			sdkInput, _ := models.ToSDKQueryInput(&input)

			b.ResetTimer()
			for range b.N {
				_, _ = db.Query(b.Context(), sdkInput)
			}
		})
	}
}

func setupDBWithGSI(b *testing.B, count int) *dynamodb.InMemoryDB {
	b.Helper()
	db := dynamodb.NewInMemoryDB()
	createInput := models.CreateTableInput{
		TableName: "BenchTable",
		KeySchema: []models.KeySchemaElement{
			{AttributeName: "id", KeyType: models.KeyTypeHash},
		},
		AttributeDefinitions: []models.AttributeDefinition{
			{AttributeName: "id", AttributeType: "S"},
			{AttributeName: "gsipk", AttributeType: "S"},
		},
		GlobalSecondaryIndexes: []models.GlobalSecondaryIndex{
			{
				IndexName: "gsi1",
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "gsipk", KeyType: models.KeyTypeHash},
				},
				Projection: models.Projection{ProjectionType: "ALL"},
			},
		},
	}
	createSDKInput := models.ToSDKCreateTableInput(&createInput)
	_, err := db.CreateTable(b.Context(), createSDKInput)
	require.NoError(b, err)

	for i := range count {
		input := models.PutItemInput{
			TableName: "BenchTable",
			Item: map[string]any{
				"id":    map[string]any{"S": strconv.Itoa(i)},
				"gsipk": map[string]any{"S": strconv.Itoa(i)},
				"val":   map[string]any{"N": strconv.Itoa(i * 10)},
			},
		}
		putSDKInput, putErr := models.ToSDKPutItemInput(&input)
		require.NoError(b, putErr)
		_, putErr = db.PutItem(b.Context(), putSDKInput)
		require.NoError(b, putErr)
	}

	return db
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
			_, _ = db.Scan(b.Context(), sdkInput)
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
		_, _ = db.PutItem(b.Context(), sdkInput)
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
				_, _ = db.GetItem(b.Context(), sdkInput)
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
				_, _ = db.PutItem(b.Context(), sdkInput)
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

	_, err := db.CreateTable(b.Context(), sdkInput)
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
		_, err = db.PutItem(b.Context(), sdkInput)
		require.NoError(b, err)
	}

	return db
}
