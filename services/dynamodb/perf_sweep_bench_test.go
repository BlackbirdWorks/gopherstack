package dynamodb_test

// Benchmarks for the gopherstack perf sweep targeting Query/Scan/TransactWriteItems
// (2026-09-19), run through the full HTTP handler (matching production request
// path: JSON decode -> dispatch -> backend -> JSON encode).

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// invokeOpB is invokeOp for benchmarks (testing.TB instead of *testing.T).
func invokeOpB(tb testing.TB, handler *dynamodb.DynamoDBHandler, action string, body any) (int, map[string]any) {
	tb.Helper()

	bodyBytes, err := json.Marshal(body)
	require.NoError(tb, err)

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+action)
	w := httptest.NewRecorder()
	_ = serveEchoHandler(handler.Handler(), w, req)

	var resp map[string]any
	if w.Body.Len() > 0 {
		require.NoError(tb, json.Unmarshal(w.Body.Bytes(), &resp))
	}

	return w.Code, resp
}

func newBenchHandlerWithGSI(tb testing.TB, tblName string) *dynamodb.DynamoDBHandler {
	tb.Helper()
	db := dynamodb.NewInMemoryDB()
	db.SetDefaultRegion("us-east-1")
	h := dynamodb.NewHandler(db)

	code, resp := invokeOpB(tb, h, "CreateTable", map[string]any{
		"TableName": tblName,
		"KeySchema": []map[string]any{
			{"AttributeName": "pk", "KeyType": "HASH"},
			{"AttributeName": "sk", "KeyType": "RANGE"},
		},
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
			{"AttributeName": "sk", "AttributeType": "S"},
			{"AttributeName": "gsipk", "AttributeType": "S"},
		},
		"GlobalSecondaryIndexes": []map[string]any{
			{
				"IndexName": "gsi1",
				"KeySchema": []map[string]any{
					{"AttributeName": "gsipk", "KeyType": "HASH"},
				},
				"Projection": map[string]any{"ProjectionType": "ALL"},
			},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.Equal(tb, 200, code, "CreateTable %s: %v", tblName, resp)

	return h
}

func seedBenchItem(tb testing.TB, h *dynamodb.DynamoDBHandler, tblName string, item map[string]any) {
	tb.Helper()
	code, resp := invokeOpB(tb, h, "PutItem", map[string]any{"TableName": tblName, "Item": item})
	require.Equal(tb, 200, code, "PutItem seed: %v", resp)
}

// BenchmarkQuery_1000Items runs Query with a KeyConditionExpression, a
// FilterExpression, and a ProjectionExpression against a single partition
// holding 1000 items, through the full HTTP handler.
func BenchmarkQuery_1000Items(b *testing.B) {
	const n = 1000
	h := newBenchHandlerWithGSI(b, "BenchQueryTable")
	for i := range n {
		seedBenchItem(b, h, "BenchQueryTable", map[string]any{
			"pk":    map[string]any{"S": "cust#1"},
			"sk":    map[string]any{"S": fmt.Sprintf("order#%04d", i)},
			"val":   map[string]any{"N": strconv.Itoa(i)},
			"gsipk": map[string]any{"S": fmt.Sprintf("g%d", i%8)},
		})
	}

	req := map[string]any{
		"TableName":              "BenchQueryTable",
		"KeyConditionExpression": "pk = :pk",
		"FilterExpression":       "val > :val",
		"ProjectionExpression":   "pk, sk, val",
		"ExpressionAttributeValues": map[string]any{
			":pk":  map[string]any{"S": "cust#1"},
			":val": map[string]any{"N": "500"},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		code, resp := invokeOpB(b, h, "Query", req)
		if code != 200 {
			b.Fatalf("Query failed: %v", resp)
		}
	}
}

// BenchmarkScan_10kItems_Filter runs Scan with a FilterExpression over a
// 10,000-item table, through the full HTTP handler.
func BenchmarkScan_10kItems_Filter(b *testing.B) {
	const n = 10000
	h := newBenchHandlerWithGSI(b, "BenchScanTable")
	for i := range n {
		seedBenchItem(b, h, "BenchScanTable", map[string]any{
			"pk":    map[string]any{"S": fmt.Sprintf("item#%05d", i)},
			"sk":    map[string]any{"S": "v1"},
			"val":   map[string]any{"N": strconv.Itoa(i)},
			"gsipk": map[string]any{"S": fmt.Sprintf("g%d", i%8)},
		})
	}

	req := map[string]any{
		"TableName":        "BenchScanTable",
		"FilterExpression": "val > :val",
		"ExpressionAttributeValues": map[string]any{
			":val": map[string]any{"N": "5000"},
		},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		code, resp := invokeOpB(b, h, "Scan", req)
		if code != 200 {
			b.Fatalf("Scan failed: %v", resp)
		}
	}
}

// BenchmarkTransactWriteItems_10 runs a 10-item TransactWriteItems (Put
// actions overwriting existing items) against a table with a GSI and 5000
// pre-existing items, through the full HTTP handler. The backing table size
// is what exercises the pre-fix O(table) secondary-index snapshot cost on
// every call; the transaction itself always touches exactly 10 items.
func BenchmarkTransactWriteItems_10(b *testing.B) {
	const tableSize = 5000
	h := newBenchHandlerWithGSI(b, "BenchTransactTable")
	for i := range tableSize {
		seedBenchItem(b, h, "BenchTransactTable", map[string]any{
			"pk":    map[string]any{"S": "item"},
			"sk":    map[string]any{"S": fmt.Sprintf("%05d", i)},
			"val":   map[string]any{"N": strconv.Itoa(i)},
			"gsipk": map[string]any{"S": fmt.Sprintf("g%d", i%8)},
		})
	}

	transactItems := make([]map[string]any, 0, 10)
	for i := range 10 {
		transactItems = append(transactItems, map[string]any{
			"Put": map[string]any{
				"TableName": "BenchTransactTable",
				"Item": map[string]any{
					"pk":    map[string]any{"S": "item"},
					"sk":    map[string]any{"S": fmt.Sprintf("%05d", i)},
					"val":   map[string]any{"N": strconv.Itoa(i)},
					"gsipk": map[string]any{"S": fmt.Sprintf("g%d", (i+1)%8)},
				},
			},
		})
	}
	req := map[string]any{"TransactItems": transactItems}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		code, resp := invokeOpB(b, h, "TransactWriteItems", req)
		if code != 200 {
			b.Fatalf("TransactWriteItems failed: %v", resp)
		}
	}
}
