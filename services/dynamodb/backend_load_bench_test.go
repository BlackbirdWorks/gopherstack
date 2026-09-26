package dynamodb_test

// Benchmarks for the 2026-09-24 profile-driven optimization pass: GSI Query
// combined with a FilterExpression, and BatchWriteItem against a large
// pre-existing table, both driven directly against the backend (no HTTP
// handler) to isolate backend cost.

import (
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// BenchmarkQuery_GSI_FilterExpression measures Query against a GSI key
// condition plus a FilterExpression that rejects most candidates, over a
// 10k-item table where the GSI partition being queried holds ~1250 items.
func BenchmarkQuery_GSI_FilterExpression(b *testing.B) {
	const size = 10000
	db := setupDBWithGSI(b, size)

	input := models.QueryInput{
		TableName:              "BenchTable",
		IndexName:              "gsi1",
		KeyConditionExpression: "gsipk = :gsipk",
		FilterExpression:       "val > :val",
		ExpressionAttributeValues: map[string]any{
			":gsipk": map[string]any{"S": strconv.Itoa(size / 2)},
			":val":   map[string]any{"N": "0"},
		},
	}
	sdkInput, err := models.ToSDKQueryInput(&input)
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, qerr := db.Query(b.Context(), sdkInput)
		if qerr != nil {
			b.Fatalf("Query failed: %v", qerr)
		}
	}
}

// BenchmarkBatchWriteItem_LargeTable measures a 25-item BatchWriteItem
// PutRequest batch (the AWS max) against a table that already holds 10k
// items, exercising the per-call lock/index-update cost at realistic table
// size rather than an empty table.
func BenchmarkBatchWriteItem_LargeTable(b *testing.B) {
	const tableSize = 10000
	db := setupDBWithItems(b, tableSize)

	const batchSize = 25
	writeReqs := make([]types.WriteRequest, 0, batchSize)
	for i := range batchSize {
		writeReqs = append(writeReqs, types.WriteRequest{
			PutRequest: &types.PutRequest{
				Item: map[string]types.AttributeValue{
					"id":  &types.AttributeValueMemberS{Value: strconv.Itoa(i)},
					"val": &types.AttributeValueMemberN{Value: strconv.Itoa(i * 100)},
				},
			},
		})
	}
	input := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{
		"BenchTable": writeReqs,
	}}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_, werr := db.BatchWriteItem(b.Context(), input)
		if werr != nil {
			b.Fatalf("BatchWriteItem failed: %v", werr)
		}
	}
}
