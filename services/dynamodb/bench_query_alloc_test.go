package dynamodb_test

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// BenchmarkQuery_OffsetKeyedItemMap measures #57 — for a known-PK query on a
// large table, only the referenced item pointers are copied (offset-keyed map)
// instead of the full Items slice.
func BenchmarkQuery_OffsetKeyedItemMap(b *testing.B) {
	tableSizes := []int{1000, 10000, 100000}

	for _, size := range tableSizes {
		b.Run(fmt.Sprintf("table=%d/pk=1", size), func(b *testing.B) {
			db := setupDBWithItems(b, size)

			// Query a single known PK — should use the sparse offset-keyed snapshot.
			input := models.QueryInput{
				TableName:              "BenchTable",
				KeyConditionExpression: "id = :id",
				ExpressionAttributeValues: map[string]any{
					":id": map[string]any{"S": strconv.Itoa(size / 2)},
				},
			}
			sdkInput, _ := models.ToSDKQueryInput(&input)

			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				_, _ = db.Query(b.Context(), sdkInput)
			}
		})

		// Full-scan fallback: unknown PK value falls back to full Items copy.
		b.Run(fmt.Sprintf("table=%d/scan_fallback", size), func(b *testing.B) {
			db := setupDBWithItems(b, size)

			// FilterExpression only, no KeyConditionExpression PK — exercises scan path.
			input := models.ScanInput{
				TableName:        "BenchTable",
				FilterExpression: "val = :v",
				ExpressionAttributeValues: map[string]any{
					":v": map[string]any{"N": strconv.Itoa(size / 2 * 10)},
				},
			}
			sdkInput, _ := models.ToSDKScanInput(&input)

			b.ResetTimer()
			b.ReportAllocs()

			for range b.N {
				_, _ = db.Scan(b.Context(), sdkInput)
			}
		})
	}
}
