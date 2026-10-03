package dynamodb_test

import "testing"

// BenchmarkUpdateItem_UpdateExpression runs UpdateItem with SET arithmetic and ADD.
func BenchmarkUpdateItem_UpdateExpression(b *testing.B) {
	h := newBenchHandlerWithGSI(b, "BenchUpdateTable")
	seedBenchItem(b, h, "BenchUpdateTable", map[string]any{
		"pk":    map[string]any{"S": "cust#1"},
		"sk":    map[string]any{"S": "order#0001"},
		"tally": map[string]any{"N": "0"},
		"gsipk": map[string]any{"S": "g0"},
		"nested": map[string]any{"M": map[string]any{
			"x": map[string]any{"N": "1"},
		}},
	})

	req := map[string]any{
		"TableName": "BenchUpdateTable",
		"Key": map[string]any{
			"pk": map[string]any{"S": "cust#1"},
			"sk": map[string]any{"S": "order#0001"},
		},
		"UpdateExpression": "SET nested.x = nested.x + :inc, updatedAt = :now ADD tally :inc",
		"ExpressionAttributeValues": map[string]any{
			":inc": map[string]any{"N": "1"},
			":now": map[string]any{"S": "2026-09-26T00:00:00Z"},
		},
	}

	b.ReportAllocs()
	for b.Loop() {
		code, resp := invokeOpB(b, h, "UpdateItem", req)
		if code != 200 {
			b.Fatalf("UpdateItem failed: %v", resp)
		}
	}
}
