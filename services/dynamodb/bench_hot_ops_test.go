package dynamodb_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func benchRawOp(b *testing.B, h *dynamodb.DynamoDBHandler, action string, body []byte) int {
	b.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+action)
	w := httptest.NewRecorder()
	_ = serveEchoHandler(h.Handler(), w, req)

	return w.Code
}

func benchMarshal(b *testing.B, v any) []byte {
	b.Helper()

	out, err := json.Marshal(v)
	if err != nil {
		b.Fatal(err)
	}

	return out
}

func benchSeedPartition(b *testing.B, h *dynamodb.DynamoDBHandler, table string, n int) {
	b.Helper()

	for i := range n {
		seedBenchItem(b, h, table, map[string]any{
			"pk":    map[string]any{"S": "cust#" + strconv.Itoa(i%10)},
			"sk":    map[string]any{"S": fmt.Sprintf("order#%05d", i)},
			"val":   map[string]any{"N": strconv.Itoa(i)},
			"gsipk": map[string]any{"S": fmt.Sprintf("g%d", i%8)},
			"data":  map[string]any{"S": "payload-payload-payload-payload"},
		})
	}
}

func BenchmarkHandlerOps(b *testing.B) {
	const table = "BenchHotOps"

	h := newBenchHandlerWithGSI(b, table)
	benchSeedPartition(b, h, table, 2000)

	putBody := func(i int) []byte {
		return benchMarshal(b, map[string]any{"TableName": table, "Item": map[string]any{
			"pk":    map[string]any{"S": "put#" + strconv.Itoa(i%100)},
			"sk":    map[string]any{"S": strconv.Itoa(i)},
			"gsipk": map[string]any{"S": "g1"},
			"val":   map[string]any{"N": strconv.Itoa(i)},
			"data":  map[string]any{"S": "payload-payload-payload-payload"},
		}})
	}

	batchBody := func(i int) []byte {
		reqs := make([]map[string]any, 25)
		for j := range reqs {
			reqs[j] = map[string]any{"PutRequest": map[string]any{"Item": map[string]any{
				"pk":    map[string]any{"S": "batch#" + strconv.Itoa(j)},
				"sk":    map[string]any{"S": strconv.Itoa(i)},
				"gsipk": map[string]any{"S": "g2"},
				"val":   map[string]any{"N": strconv.Itoa(j)},
			}}}
		}

		return benchMarshal(b, map[string]any{"RequestItems": map[string]any{table: reqs}})
	}

	fixed := map[string][]byte{
		"Query_KeyCond_Filter": benchMarshal(b, map[string]any{
			"TableName":                table,
			"KeyConditionExpression":   "pk = :pk AND begins_with(sk, :p)",
			"FilterExpression":         "val > :v AND attribute_exists(#d)",
			"ExpressionAttributeNames": map[string]any{"#d": "data"},
			"ExpressionAttributeValues": map[string]any{
				":pk": map[string]any{"S": "cust#3"},
				":p":  map[string]any{"S": "order#"},
				":v":  map[string]any{"N": "500"},
			},
		}),
		"Scan_Filter": benchMarshal(b, map[string]any{
			"TableName":        table,
			"FilterExpression": "val BETWEEN :a AND :b",
			"ExpressionAttributeValues": map[string]any{
				":a": map[string]any{"N": "100"},
				":b": map[string]any{"N": "200"},
			},
		}),
		"UpdateItem_Expr": benchMarshal(b, map[string]any{
			"TableName": table,
			"Key": map[string]any{
				"pk": map[string]any{"S": "cust#1"},
				"sk": map[string]any{"S": "order#00001"},
			},
			"UpdateExpression": "SET val = val + :inc, updatedAt = :now ADD cnt :inc",
			"ExpressionAttributeValues": map[string]any{
				":inc": map[string]any{"N": "1"}, ":now": map[string]any{"S": "2026-10-04T00:00:00Z"},
			},
		}),
		"GetItem": benchMarshal(b, map[string]any{
			"TableName": table,
			"Key":       map[string]any{"pk": map[string]any{"S": "cust#1"}, "sk": map[string]any{"S": "order#00001"}},
		}),
	}

	actions := map[string]string{
		"Query_KeyCond_Filter": "Query", "Scan_Filter": "Scan", "UpdateItem_Expr": "UpdateItem", "GetItem": "GetItem",
	}

	for name, body := range fixed {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()

			for b.Loop() {
				if code := benchRawOp(b, h, actions[name], body); code != http.StatusOK {
					b.Fatalf("status %d", code)
				}
			}
		})
	}

	b.Run("PutItem", func(b *testing.B) {
		bodies := make([][]byte, 256)
		for i := range bodies {
			bodies[i] = putBody(i)
		}

		b.ReportAllocs()

		i := 0
		for b.Loop() {
			if code := benchRawOp(b, h, "PutItem", bodies[i%len(bodies)]); code != http.StatusOK {
				b.Fatalf("status %d", code)
			}

			i++
		}
	})

	b.Run("BatchWriteItem_25", func(b *testing.B) {
		bodies := make([][]byte, 64)
		for i := range bodies {
			bodies[i] = batchBody(i)
		}

		b.ReportAllocs()

		i := 0
		for b.Loop() {
			if code := benchRawOp(b, h, "BatchWriteItem", bodies[i%len(bodies)]); code != http.StatusOK {
				b.Fatalf("status %d", code)
			}

			i++
		}
	})
}
